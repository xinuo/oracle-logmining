package pub.timelyrain.logmining.utils;

public class ValueConverter {
    /**
     * 将16进制的ANSI格式的unicode码 解码
     *
     * @param unicode
     * @param split   oracle使用的unicode分隔符为 \  入参为\\\\  ,Java使用的分隔符为\\\\u
     * @return
     */
    public static String unicodeToStr(String unicode, String split) {
        if (unicode == null)
            return null;

        String[] charsHex = unicode.split(split);
        StringBuilder result = new StringBuilder();

        if (charsHex.length == 1)
            return charsHex[0];

        for (int i = 1; i < charsHex.length; i++) {
            String charHex = charsHex[i];
            int hex = Integer.parseInt(charHex.substring(0, 4), 16);
            char c = (char) hex;
            result.append(c).append(charHex.substring(4));
        }
        result.insert(0, charsHex[0]);
        return result.toString();

    }

    public static void main(String[] args) {
        String a = "UNISTR('aa\\4F55bb\\4E16cc\\6C11dd')";
        if (a.startsWith("UNISTR('") && a.endsWith("')"))
            a = a.substring(8, a.length() - 2);
        System.out.println(a);

        System.out.println(unicodeToStr(a, "\\\\"));


    }
}
