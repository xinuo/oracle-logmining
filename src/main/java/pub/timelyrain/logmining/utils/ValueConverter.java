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

        for (String charHex : charsHex) {
            if(charHex.length() == 0)
                continue;
            int hex = Integer.parseInt(charHex, 16);
            char c = (char) hex;
            result.append(c);
        }

        return result.toString();

    }

    public static void main(String[] args) {
        String a = "UNISTR('\\4F55\\4E16\\6C11')";
        if(a.startsWith("UNISTR('") && a.endsWith("')"))
            a = a.substring(8,a.length()-2);
        System.out.println(a);
        System.out.println(unicodeToStr(a,"\\\\"));


    }
}
