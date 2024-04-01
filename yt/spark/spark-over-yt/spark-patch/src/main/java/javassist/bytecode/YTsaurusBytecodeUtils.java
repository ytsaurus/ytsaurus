package javassist.bytecode;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

import static javassist.bytecode.ConstPool.CONST_String;

public class YTsaurusBytecodeUtils {
    private YTsaurusBytecodeUtils() {}

    public static void updateUtf8Constant(ConstPool cp, int constId, String newValue) {
        Utf8Info item = (Utf8Info) cp.getItem(constId);
        if (item == null) {
            throw new IllegalArgumentException("Const at id " + constId + " was not found");
        }

        item.string = newValue;
    }

    public static int findStringConstant(ConstPool cp, Predicate<String> predicate) {
        for(int i = 1; i < cp.getSize(); i++) {
            int tag = cp.getTag(i);
            if (tag == CONST_String && predicate.test(cp.getStringInfo(i))) {
                return i;
            }
        }
        throw new NoSuchElementException("Can't find a string constant that satisfies supplied predicate");
    }

    public static int getUtf8ConstantId(ConstPool cp, int index) {
        ConstInfo ci = cp.getItem(index);
        if (ci instanceof StringInfo) {
            return ((StringInfo) ci).string;
        } else {
            throw new IllegalArgumentException("getUtf8ConstantId is applicable only for string constants");
        }
    }

    public static StackMapTable addStackMapTableFrames(
            StackMapTable sm,
            List<Integer> positions,
            int start,
            int insertLength) throws BadBytecode {
        StackMapTable.SimpleCopy walker = new StackMapTableFrameAdder(sm.get(), positions, start, insertLength);
        byte[] result = walker.doit();
        return new StackMapTable(sm.constPool, result);
    }

    private static class StackMapTableFrameAdder extends StackMapTable.SimpleCopy {

        private int offset;
        private boolean processed;
        private final int start;
        private final int insertLength;
        private final List<Integer> positions;

        public StackMapTableFrameAdder(byte[] data, List<Integer> positions, int start, int insertLength) {
            super(data);
            offset = -1;
            processed = false;
            this.positions = positions;
            this.start = start;
            this.insertLength = insertLength;
        }

        private int incOffset(int delta) {
            offset += delta + 1;
            if (!processed && offset > start) {
                int prevOffset = offset - delta - 1;
                for (int position : positions) {
                    super.sameFrame(0, position - prevOffset - 1);
                    prevOffset = position;
                }
                processed = true;
                return delta - insertLength;
            }
            return delta;
        }

        @Override
        public void sameFrame(int pos, int offsetDelta) {
            super.sameFrame(pos, incOffset(offsetDelta));
        }

        @Override
        public void sameLocals(int pos, int offsetDelta, int stackTag, int stackData) {
            super.sameLocals(pos, incOffset(offsetDelta), stackTag, stackData);
        }

        @Override
        public void chopFrame(int pos, int offsetDelta, int k) {
            super.chopFrame(pos, incOffset(offsetDelta), k);
        }

        @Override
        public void appendFrame(int pos, int offsetDelta, int[] tags, int[] data) {
            super.appendFrame(pos, incOffset(offsetDelta), tags, data);
        }

        @Override
        public void fullFrame(int pos, int offsetDelta, int[] localTags, int[] localData, int[] stackTags, int[] stackData) {
            super.fullFrame(pos, incOffset(offsetDelta), localTags, localData, stackTags, stackData);
        }
    }
}
