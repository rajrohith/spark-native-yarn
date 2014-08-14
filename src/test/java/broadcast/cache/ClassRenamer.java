package broadcast.cache;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.commons.Remapper;
import org.objectweb.asm.commons.RemappingClassAdapter;

public class ClassRenamer {

	/**
	 * @param to
	 * @param classBytes
	 * @return
	 */
	public static byte[] renameTo(final String newName, byte[] classBytes) {
		ClassReader creader = new ClassReader(classBytes);
		ClassWriter writer = new ClassWriter(ClassWriter.COMPUTE_MAXS);
		RemappingClassAdapter visitor = new RemappingClassAdapter(writer, new Remapper() {}) {

			@Override
			public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
				System.out.println("Visiting type " + name);

				super.visit(version, access, newName, remapper.mapSignature(signature, false), remapper.mapType(superName),
						interfaces == null ? null : remapper .mapTypes(interfaces));
			}
		};
		creader.accept(visitor, 0);
		return writer.toByteArray();
	}
}