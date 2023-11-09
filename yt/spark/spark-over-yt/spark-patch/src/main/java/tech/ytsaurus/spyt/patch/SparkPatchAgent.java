package tech.ytsaurus.spyt.patch;

import javassist.bytecode.ClassFile;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class SparkPatchAgent {

    private static final Logger log = LoggerFactory.getLogger(SparkPatchAgent.class);

    public static void premain(String args, Instrumentation inst) {
        log.info("Starting SparkPatchAgent for hooking on jvm classloader");
        String patchJarPath = ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
                .filter(arg -> arg.contains("spark-yt-spark-patch.jar"))
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
            throw new RuntimeException(e);
        }
    }
}

class SparkPatchClassTransformer implements ClassFileTransformer {

    private static final Logger log = LoggerFactory.getLogger(SparkPatchAgent.class);
    private static final String PKG_SUFFIX = "/spyt/patch";
    private final Map<String, String> classMappings;
    private final Set<String> patchedClasses;

    static String toPatchClassName(String className) {
        int lastSlashPos = className.lastIndexOf('/');
        String basePackage = className.substring(0, lastSlashPos);
        String simpleClassName = className.substring(lastSlashPos + 1);
        String patchPackage = basePackage + PKG_SUFFIX;
        return patchPackage + "/" + simpleClassName + ".class";
    }

    static String toRealClassName(String patchClassName) {
        return patchClassName.replace(PKG_SUFFIX, "");
    }

    SparkPatchClassTransformer(Map<String, String> classMappings) {
        this.classMappings = classMappings;
        this.patchedClasses = new HashSet<>(classMappings.values());
        log.debug("Creating classfile transformer for the following classes: {}", patchedClasses);
    }


    @Override
    public byte[] transform(
            ClassLoader loader,
            String className,
            Class<?> classBeingRedefined,
            ProtectionDomain protectionDomain,
            byte[] classfileBuffer) throws IllegalClassFormatException {
        if (patchedClasses.contains(className)) {
            try {
                byte[] patchSourceBytes = IOUtils.resourceToByteArray(toPatchClassName(className), this.getClass().getClassLoader());

                ClassFile cf = new ClassFile(new DataInputStream(new ByteArrayInputStream(patchSourceBytes)));
                cf.renameClass(classMappings);
                ByteArrayOutputStream patchedBytesOutputStream = new ByteArrayOutputStream();
                cf.write(new DataOutputStream(patchedBytesOutputStream));
                patchedBytesOutputStream.flush();
                byte[] patchedBytes = patchedBytesOutputStream.toByteArray();

                log.info("Patch size for class {} is {} and after patching the size is {}",
                        className, patchSourceBytes.length, patchedBytes.length);

                return patchedBytes;
            } catch (IOException e) {
                log.error("No patch for class " + className, e);
            }
        }

        return null;
    }
}