package broadcast.cache;

import org.objectweb.asm.commons.Remapper;

public class StringRemapper extends Remapper {
	
	public String remapStringConstant(String constant) {
		//by default, don't re-map anything
		return constant;
	}

}
