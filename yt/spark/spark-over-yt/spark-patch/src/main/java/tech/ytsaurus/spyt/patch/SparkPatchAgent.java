package tech.ytsaurus.spyt.patch;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.bytecode.ClassFile;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.spyt.patch.annotations.Subclass;

import java.io.*;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.lang.instrument.Instrumentation;
import java.lang.management.ManagementFactory;
import java.security.ProtectionDomain;
import java.util.*;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;

/**
 * Strategies for patching Spark classes:
 *
 * <ol>
 * <li>Completely replace class bytecode with provided implementation. This is the default strategy. The patched
 * class implementation must have the same name as original class and should be placed into
 * "original package".spyt.patch package in this module.</li>
 *
 * <li>Replace with subclass. In this strategy the original class is preserved but renamed to "original name"Base
 * at runtime. The patched class should be placed into the same package as the original class and annotated with
 * {@link tech.ytsaurus.spyt.patch.annotations.Subclass} annotation that should be parameterized with full name of the
 * original class. At runtime the patched class is renamed to "original name" and has "original name"Base superclass
 * which is actually the original class before patching</li>
 * </ol>
 */
public class SparkPatchAgent {

    private static final Logger log = LoggerFactory.getLogger(SparkPatchAgent.class);

    public static void premain(String args, Instrumentation inst) {
        log.info("Starting SparkPatchAgent for hooking on jvm classloader");
        String patchJarPath = ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
                .filter(arg -> arg.contains("spark-yt-spark-patch"))
                .map(arg -> arg.substring(arg.indexOf(':') + 1))
                .findFirst()
                .orElseThrow();

        try(JarFile patchJarFile = new JarFile(patchJarPath)) {
            Map<String,String> classMappings = patchJarFile
                    .versionedStream()
                    .map(ZipEntry::getName)
                    .filter(fileName -> fileName.endsWith(".class") && !fileName.startsWith("tech/ytsaurus/"))
                    .map(fileName -> fileName.substring(0, fileName.length() - 6))
                    .collect(Collectors.toMap(s -> s, SparkPatchClassTransformer::toRealClassName));

            inst.addTransformer(new SparkPatchClassTransformer(classMappings));
        } catch (IOException e) {
            throw new SparkPatchException(e);
        }
    }
}

class SparkPatchClassTransformer implements ClassFileTransformer {

    private static final Logger log = LoggerFactory.getLogger(SparkPatchAgent.class);
    private static final String PKG_SUFFIX = "/spyt/patch";
    private final Map<String, String> classMappings;
    private final Map<String, String> patchedClasses;

    static String toRealClassName(String patchClassName) {
        if (patchClassName.contains(PKG_SUFFIX)) {
            return patchClassName.replace(PKG_SUFFIX, "");
        }

        try {
            ClassFile classFile = loadClassFile(patchClassName + ".class");
            CtClass ctClass = ClassPool.getDefault().makeClass(classFile);
            Subclass subclassAnnotation = (Subclass) ctClass.getAnnotation(Subclass.class);
            if (subclassAnnotation != null) {
                String baseClass = subclassAnnotation.value();
                return baseClass.replace('.', File.separatorChar);
            }
            throw new SparkPatchException("Unable to apply patch from class " + patchClassName);
        } catch (ClassNotFoundException | IOException e) {
            throw new SparkPatchException(e);
        }
    }

    static ClassFile loadClassFile(byte[] classBytes) throws IOException {
        return new ClassFile(new DataInputStream(new ByteArrayInputStream(classBytes)));
    }
    static ClassFile loadClassFile(String classFile) throws IOException {
        byte[] patchSourceBytes = IOUtils.resourceToByteArray(classFile, SparkPatchClassTransformer.class.getClassLoader());
        return loadClassFile(patchSourceBytes);
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
            byte[] classfileBuffer) throws IllegalClassFormatException {
        if (!patchedClasses.containsKey(className)) {
            return null;
        }

        try {
            ClassFile cf = loadClassFile(toPatchClassName(className));
            processAnnotations(cf, loader, classfileBuffer);
            cf.renameClass(classMappings);
            byte[] patchedBytes = serializeClass(cf);

            log.info("Patch size for class {} is {} and after patching the size is {}",
                    className, classfileBuffer.length, patchedBytes.length);

            return patchedBytes;
        } catch (IOException | ClassNotFoundException | CannotCompileException e) {
            log.error("No patch for class " + className, e);
            return null;
        }
    }

    private String toPatchClassName(String className) {
        return patchedClasses.get(className) + ".class";
    }

    private void processAnnotations(ClassFile cf, ClassLoader loader, byte[] baseClassBytes)
            throws ClassNotFoundException, CannotCompileException, IOException {
        CtClass ctClass = ClassPool.getDefault().makeClass(cf);
        for (Object annotation : ctClass.getAnnotations()) {
            if (annotation instanceof Subclass) {
                Subclass subclass = (Subclass) annotation;
                String thisClass = subclass.value();
                String baseClass = subclass.value() + "Base";
                log.info("Changing superclass of {} to {}", thisClass, baseClass);
                cf.setSuperclass(baseClass);
                cf.renameClass(thisClass, baseClass);

                ClassFile baseCf = loadClassFile(baseClassBytes);
                baseCf.renameClass(thisClass, baseClass);
                ClassPool.getDefault().makeClass(baseCf).toClass(loader, null);
            }
        }
    }
}