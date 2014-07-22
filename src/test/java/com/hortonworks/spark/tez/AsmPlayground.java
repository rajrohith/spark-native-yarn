package com.hortonworks.spark.tez;

import java.net.URLClassLoader;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

public class AsmPlayground {

	public static void main(String[] args) throws Exception {
		
		 ClassWriter cw = new ClassWriter(0);
	        FieldVisitor fv;
	        MethodVisitor mv;
	        AnnotationVisitor av0;

	        cw.visit(Opcodes.V1_6, Opcodes.ACC_PUBLIC + Opcodes.ACC_SUPER, "Testing", null, "java/lang/Object", null);

	        cw.visitSource("Testing.java", null);

	        {
	            mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
	            mv.visitCode();
	            Label l0 = new Label();
	            mv.visitLabel(l0);
	            mv.visitLineNumber(1, l0);
	            mv.visitVarInsn(Opcodes.ALOAD, 0);
	            mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V");
	            mv.visitInsn(Opcodes.RETURN);
	            Label l1 = new Label();
	            mv.visitLabel(l1);
	            mv.visitLocalVariable("this", "LTesting;", null, l0, l1, 0);
	            mv.visitMaxs(1, 1);
	            mv.visitEnd();
	        }
	        {
	            mv = cw.visitMethod(Opcodes.ACC_PUBLIC + Opcodes.ACC_STATIC, "main", "([Ljava/lang/String;)V", null, null);
	            mv.visitCode();
	            Label l0 = new Label();
	            mv.visitLabel(l0);
	            mv.visitLineNumber(3, l0);
	            mv.visitFieldInsn(Opcodes.GETSTATIC, "java/lang/System", "out", "Ljava/io/PrintStream;");
	            mv.visitLdcInsn("Works!");
	            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/io/PrintStream", "println", "(Ljava/lang/String;)V");
	            Label l1 = new Label();
	            mv.visitLabel(l1);
	            mv.visitLineNumber(4, l1);
	            mv.visitInsn(Opcodes.RETURN);
	            Label l2 = new Label();
	            mv.visitLabel(l2);
	            mv.visitLocalVariable("args", "[Ljava/lang/String;", null, l0, l2, 0);
	            mv.visitMaxs(2, 1);
	            mv.visitEnd();
	        }
	        cw.visitEnd();
	        
	        byte[] bytes = cw.toByteArray();
	        ByteClassLoader cl = new ByteClassLoader(ClassLoader.getSystemClassLoader());
	        Class clazz = cl.load(bytes);
	        System.out.println(clazz);
//	        return cw.toByteArray();
	}

	public static class ByteClassLoader extends URLClassLoader {

		public ByteClassLoader(ClassLoader parent) {
			super(((URLClassLoader) ClassLoader.getSystemClassLoader())
					.getURLs(), parent);
		}

		// @Override
		// protected Class<?> findClass(final String name) throws
		// ClassNotFoundException {
		// byte[] classBytes = this.extraClassDefs.remove(name);
		// if (classBytes != null) {
		// return defineClass(name, classBytes, 0, classBytes.length);
		// }
		// return super.findClass(name);
		// }

		public Class<?> load(byte[] classBytes) {
			return defineClass("Testing", classBytes, 0,
					classBytes.length);
		}

	}
}

//ClassVisitor checker = new ClassVisitor(Opcodes.ASM4) {
//	@Override
//	public MethodVisitor visitMethod(int access, String name,
//			String desc, String signature, String[] exceptions) {
//		System.out.println("Method: " + name + " " + desc);
//		return super.visitMethod(access, name, desc, signature,
//				exceptions);
//	}
//
//};
//InputStream in = AsmPlayground.class
//		.getResourceAsStream("/com/hortonworks/tez/spark/Foo.class");
//ClassReader classReader = new ClassReader(in);
//classReader.accept(checker, 0);
//
//ClassWriter classWriter = new ClassWriter(classReader, 1);
//
//String methodName = "setFoo";
//
//MethodVisitor mv = classWriter.visitMethod(Opcodes.ACC_PUBLIC,
//		methodName, "(Ljava/lang/String;)V", null, null);
//mv.visitVarInsn(Opcodes.ALOAD, 0);
//mv.visitVarInsn(Type.getType(Foo.class).getOpcode(Opcodes.ILOAD), 1);
//
//mv.visitFieldInsn(Opcodes.PUTFIELD, "com/hortonworks/tez/spark/Foo", "foo", "Ljava/lang/String;");
//mv.visitInsn(Opcodes.RETURN);
//mv.visitMaxs(0, 0);
//
//byte[] classBytes = classWriter.toByteArray();
//
//System.out.println(new String(classBytes));
//
//ByteClassLoader cl = new ByteClassLoader(
//		ClassLoader.getSystemClassLoader());
//Class clazz = cl.load(classBytes);
