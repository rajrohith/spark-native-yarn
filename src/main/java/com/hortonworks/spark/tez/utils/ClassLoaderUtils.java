package com.hortonworks.spark.tez.utils;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Field;
import java.net.URI;

import org.apache.spark.rdd.RDD;
import org.objectweb.asm.commons.Remapper;
import org.springframework.util.ClassUtils;

import scala.tools.asm.ClassReader;
import scala.tools.asm.ClassVisitor;
import scala.tools.asm.ClassWriter;
import scala.tools.asm.Opcodes;
import sun.misc.Unsafe;

public class ClassLoaderUtils {
	
	private static final Unsafe unsafe = getUnsafe();
	
	/**
	 * 
	 * @param classBytes
	 * @param fqn
	 */
	public static void loadClassFromBytesAs(byte[] classBytes, String fqn) {
		//System.out.println("In bytes: " + classBytes.length);
		String newClassName = ClassUtils.convertClassNameToResourcePath(fqn);
		byte[] modifiedClassBytes = renameTo(newClassName, classBytes);
		//System.out.println("Out bytes: " + modifiedClassBytes.length);
		unsafe.defineClass(null, modifiedClassBytes, 0, modifiedClassBytes.length, RDD.class.getClassLoader(), RDD.class.getProtectionDomain());
	}
	
	/**
	 * 
	 * @param clazz
	 * @param fqn
	 */
	public static void loadClassAs(Class<?> clazz, String fqn) {
		long start = System.currentTimeMillis();
		try {
			byte[] classBytes = getClassContent(clazz);
			loadClassFromBytesAs(classBytes, fqn);
		} catch (Exception e) {
			throw new IllegalArgumentException("Failed load class " + clazz + " as " + fqn, e);
		}
		long stop = System.currentTimeMillis();
		System.out.println(stop-start);
	}
	
	/**
	 * 
	 * @param clazz
	 * @return
	 * @throws Exception
	 */
	public static byte[] getClassContent(Class<?> clazz) throws Exception {
		URI uri = clazz.getProtectionDomain().getCodeSource().getLocation().toURI();
		if (uri == null){
			throw new ClassNotFoundException("Failed to find source location for class " + clazz);
		}
	    File base = new File(uri);
	    File classFile = new File(base, ClassUtils.convertClassNameToResourcePath(clazz.getName()) + ".class");
	    FileInputStream input = new FileInputStream(classFile);
	    byte[] content = new byte[(int)classFile.length()];
	    input.read(content);
	    input.close();
	    return content;
	}
	
	/**
	 * 
	 * @param newName
	 * @param classBytes
	 * @return
	 */
	private static byte[] renameTo(final String newName, byte[] classBytes) {
		ClassReader creader = new ClassReader(classBytes);
		
		ClassWriter writer = new ClassWriter(ClassWriter.COMPUTE_MAXS);
		
		
		ClassVisitor cv = new ClassVisitor(Opcodes.ASM4, writer) {
			Remapper remapper = new Remapper() {};
			@Override
			public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
				System.out.println("Visiting type " + name);
				
				super.visit(version, access, newName, remapper.mapSignature(signature, false), remapper.mapType(superName),
						interfaces == null ? null : remapper .mapTypes(interfaces));
			}
		};
		creader.accept(cv, 0);
		return writer.toByteArray();
	}
	
	/**
	 * 
	 * @return
	 */
	private static Unsafe getUnsafe() {
		try {
			Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			field.setAccessible(true);
			return (sun.misc.Unsafe) field.get(null);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}
}
