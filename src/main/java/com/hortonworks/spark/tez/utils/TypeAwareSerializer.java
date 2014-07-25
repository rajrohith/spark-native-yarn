package com.hortonworks.spark.tez.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.net.URLClassLoader;

import org.apache.commons.io.IOUtils;

public class TypeAwareSerializer {
	public static class TypeAwareObjectOutputStream extends ObjectOutputStream {
		
		private final Class<?> interpreterCl;

		public TypeAwareObjectOutputStream(OutputStream out) throws IOException {
			super(out);
			try {
				this.interpreterCl = Class.forName("scala.tools.nsc.interpreter.AbstractFileClassLoader");
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
		
		@Override
		protected void annotateClass(Class<?> cl) throws IOException {
			System.out.println("Checking if class serialization is required for " + cl.getName());
						
			ClassLoader classLoader = cl.getClassLoader();
			boolean extramagic = classLoader != null && this.isReplClassLoader(cl.getClassLoader());
			writeBoolean(extramagic);
				
			if (extramagic){
				System.out.println("Annotating: " + cl);
				String classAsPath = cl.getName().replace('.', '/') + ".class";
				InputStream is = classLoader.getResourceAsStream(classAsPath);
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				IOUtils.copy(is, bos);
				byte[] classBytes = bos.toByteArray();
				writeInt(classBytes.length);
			    write(classBytes);
			}
		}
		
		private boolean isReplClassLoader(ClassLoader cl) {
			if (this.interpreterCl.isAssignableFrom(cl.getClass())){
				return true;
			}
			return false;
		}
	}
	
	public static class TypeAwareObjectInputStream extends ObjectInputStream {
		
		public TypeAwareObjectInputStream(InputStream in) throws IOException {
			super(in);
		}
		
		@Override
		protected Class<?> resolveClass(ObjectStreamClass desc) {
			Class<?> clazz = null;
			try {
				if (this.readBoolean()){
					System.out.println("Class bytes are serialized");
					ClassLoader classLoader = this.getClass().getClassLoader();
					String name = desc.getName();
					int length = readInt();
					byte[] classBytes = new byte[length];
					readFully(classBytes);
					ByteReadingClassLoader brClassLoader = new ByteReadingClassLoader((URLClassLoader) classLoader, classBytes, name);
					clazz = brClassLoader.loadClass(name);
				}		
				else {
					clazz = super.resolveClass(desc);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			return clazz;
		}
		
		private static class ByteReadingClassLoader extends URLClassLoader {
			private final String name;
			private final byte[] classBytes;
			public ByteReadingClassLoader(URLClassLoader classLoader, byte[] classBytes, String name) {
				super(classLoader.getURLs(), classLoader);
				this.classBytes = classBytes;
				this.name = name;
			}
			
			@Override
			protected Class<?> findClass(String className) throws ClassNotFoundException {
				if (this.name.equals(className)){
					return this.defineClass(name, this.classBytes, 0, this.classBytes.length);
				}
				else {
					return super.findClass(className);
				}
			}
		}
	}
}
