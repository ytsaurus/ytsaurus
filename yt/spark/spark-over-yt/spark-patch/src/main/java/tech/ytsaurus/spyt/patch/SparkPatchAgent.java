package tech.ytsaurus.spyt.patch;

import javassist.*;
import javassist.bytecode.ClassFile;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.shaded.com.google.common.collect.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.spyt.patch.annotations.Decorate;
import tech.ytsaurus.spyt.patch.annotations.DecoratedMethod;
import tech.ytsaurus.spyt.patch.annotations.OriginClass;
import tech.ytsaurus.spyt.patch.annotations.Subclass;

import java.io.*;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.security.ProtectionDomain;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;

/**
 * Strategies for patching Spark classes:
 *
 * <ol>
 * <li>Completely replace class bytecode with provided implementation. This is the default strategy. The patched
 * class implementation must be annotated only with {@link OriginClass} annotation that should be parameterized with
 * full name of the original class.</li>
 *
 * <li>Replace with subclass. In this strategy the original class is preserved but renamed to "original name"Base
 * at runtime. The patched class should be placed into the same package as the original class and annotated with
 * {@link Subclass} and {@link OriginClass} annotations. The latter should be parameterized with full name of the
 * original class. At runtime the patched class is renamed to "original name" and has "original name"Base superclass
 * which is actually the original class before patching</li>
 *
 * <li>Decorate methods. In this strategy the base class body is generally preserved but is enriched with additional
 * decorating methods which are defined in decorator class. The decorator class should be annotated with
 * {@link Decorate} and {@link OriginClass} annotations. The latter should be parameterized with full name of the
 * original class. In the decorating class the methods should also be annotated with {@link DecoratedMethod} annotation.
 * The method should has the same name and signature as the original method. Also there may be a stub method that
 * has the same signature and the same name that is prefixed with __ when it is required to call original method from
 * the decorating method.</li>
 * </ol>
 */
public class SparkPatchAgent {

    private static final Logger log = LoggerFactory.getLogger(SparkPatchAgent.class);

    public static void premain(String args, Instrumentation inst) {
        log.info("Starting SparkPatchAgent for hooking on jvm classloader");

        String patchJarPath = ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
                .filter(arg -> arg.startsWith("-javaagent") && arg.contains("spark-yt-spark-patch"))
                .map(arg -> arg.substring(arg.indexOf(':') + 1))
                .findFirst()
                .orElseThrow();

        try(JarFile patchJarFile = new JarFile(patchJarPath)) {
            Stream<String> localClasses = patchJarFile.versionedStream()
                    .map(ZipEntry::getName)
                    .filter(fileName -> fileName.endsWith(".class") && !fileName.startsWith("tech/ytsaurus/"));

            Stream<String> externalClasses =
                    IOUtils.resourceToString("/externalClasses.txt", Charset.defaultCharset()).lines()
                            .filter(line -> !line.isBlank())
                            .map(className -> className + ".class");

            Map<String,String> classMappings = Streams.concat(localClasses, externalClasses)
                    .flatMap(fileName -> SparkPatchClassTransformer.toOriginClassName(fileName).stream())
                    .collect(Collectors.toMap(s -> s[0], s -> s[1]));

            inst.addTransformer(new SparkPatchClassTransformer(classMappings));
        } catch (IOException e) {
            throw new SparkPatchException(e);
        }
    }
}

class SparkPatchClassTransformer implements ClassFileTransformer {

    private static final Logger log = LoggerFactory.getLogger(SparkPatchAgent.class);
    private final Map<String, String> classMappings;
    private final Map<String, String> patchedClasses;

    static Optional<String[]> toOriginClassName(String fileName) {
        try {
            String patchClassName = fileName.substring(0, fileName.length() - 6);
            Optional<ClassFile> optClassFile = loadClassFile(fileName);
            if (optClassFile.isEmpty()) {
                return Optional.empty();
            }
            ClassFile classFile = optClassFile.get();
            CtClass ctClass = ClassPool.getDefault().makeClass(classFile);
            String originClass = getOriginClass(ctClass);
            if (originClass != null) {
                return Optional.of(new String[] {patchClassName, originClass.replace('.', File.separatorChar)});
            }
            return Optional.empty();
        } catch (ClassNotFoundException | IOException e) {
            throw new SparkPatchException(e);
        }
    }

    static String getOriginClass(CtClass ctClass) throws ClassNotFoundException {
        OriginClass originClassAnnotaion = (OriginClass) ctClass.getAnnotation(OriginClass.class);
        if (originClassAnnotaion != null) {
            String originClass = originClassAnnotaion.value();
            if (ctClass.getName().endsWith("$") && !originClass.endsWith("$")) {
                originClass = originClass + "$";
            }
            return originClass;
        }
        return null;
    }

    static ClassFile loadClassFile(byte[] classBytes) throws IOException {
        return new ClassFile(new DataInputStream(new ByteArrayInputStream(classBytes)));
    }
    static Optional<ClassFile> loadClassFile(String classFile) throws IOException {
        ClassLoader classLoader = SparkPatchClassTransformer.class.getClassLoader();
        if (classLoader.getResource(classFile) == null) {
            return Optional.empty();
        }
        byte[] patchSourceBytes = IOUtils.resourceToByteArray(classFile, classLoader);
        return Optional.of(loadClassFile(patchSourceBytes));
    }

    static byte[] serializeClass(ClassFile cf) throws IOException {
        ByteArrayOutputStream patchedBytesOutputStream = new ByteArrayOutputStream();
        cf.write(new DataOutputStream(patchedBytesOutputStream));
        patchedBytesOutputStream.flush();
        return patchedBytesOutputStream.toByteArray();
    }

    SparkPatchClassTransformer(Map<String, String> classMappings) {
        this.classMappings = classMappings;
        this.patchedClasses = classMappings
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        log.debug("Creating classfile transformer for the following classes: {}", patchedClasses);
    }

    @Override
    public byte[] transform(
            ClassLoader loader,
            String className,
            Class<?> classBeingRedefined,
            ProtectionDomain protectionDomain,
            byte[] classfileBuffer) {
        if (!patchedClasses.containsKey(className)) {
            return null;
        }

        try {
            ClassFile cf = loadClassFile(toPatchClassName(className)).orElseThrow();
            cf = processAnnotations(cf, loader, classfileBuffer);
            cf.renameClass(classMappings);
            byte[] patchedBytes = serializeClass(cf);

            log.info("Patch size for class {} is {} and after patching the size is {}",
                    className, classfileBuffer.length, patchedBytes.length);

            return patchedBytes;
        } catch (IOException | ClassNotFoundException | CannotCompileException |
                 NotFoundException | NoSuchElementException e) {
            log.error("No patch for class " + className, e);
            return null;
        }
    }

    private String toPatchClassName(String className) {
        return patchedClasses.get(className) + ".class";
    }

    private ClassFile processAnnotations(ClassFile cf, ClassLoader loader, byte[] baseClassBytes)
            throws ClassNotFoundException, CannotCompileException, IOException, NotFoundException {
        CtClass ctClass = ClassPool.getDefault().makeClass(cf);
        String originClass = getOriginClass(ctClass);
        if (originClass == null) {
            return cf;
        }

        ClassFile processedClassFile = cf;

        for (Object annotation : ctClass.getAnnotations()) {
            if (annotation instanceof Subclass) {
                String baseClass = originClass + "Base";
                log.info("Changing superclass of {} to {}", originClass, baseClass);
                cf.setSuperclass(baseClass);
                cf.renameClass(originClass, baseClass);

                ClassFile baseCf = loadClassFile(baseClassBytes);
                baseCf.renameClass(originClass, baseClass);
                ClassPool.getDefault().makeClass(baseCf).toClass(loader, null);
            }

            if (annotation instanceof Decorate) {
                ClassFile baseCf = loadClassFile(baseClassBytes);
                CtClass baseCtClass = ClassPool.getDefault().makeClass(baseCf);
                for (CtMethod method : ctClass.getDeclaredMethods()) {
                    if (method.hasAnnotation(DecoratedMethod.class)) {
                        DecoratedMethod dmAnnotation = (DecoratedMethod) method.getAnnotation(DecoratedMethod.class);
                        String methodName = dmAnnotation.name().isEmpty() ? method.getName() : dmAnnotation.name();
                        String methodSignature = dmAnnotation.signature();
                        CtMethod baseMethod = methodSignature.isEmpty()
                                ? baseCtClass.getDeclaredMethod(methodName)
                                : baseCtClass.getMethod(methodName, methodSignature);
                        log.debug("Patching decorated method {}", methodName);
                        String innerMethodName = "__" + methodName;
                        baseMethod.setName(innerMethodName);

                        CtMethod newMethod = CtNewMethod.copy(method, baseCtClass, null);
                        baseCtClass.addMethod(newMethod);
                    }
                }

                processedClassFile = baseCtClass.getClassFile();
            }
        }

        return processedClassFile;
    }
}