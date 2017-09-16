package fdbs;

public class UnicodeManager {
    public static String replaceUnicodesWithChars(String query) {
        if (query.contains(getUnicodeFromCharForReplacing("�"))) {
            query = query.replaceAll(getUnicodeFromCharForReplacing("�"), "�");
        }
        if (query.contains(getUnicodeFromCharForReplacing("�"))) {
            query = query.replaceAll(getUnicodeFromCharForReplacing("�"), "�");
        }
        if (query.contains(getUnicodeFromCharForReplacing("�"))) {
            query = query.replaceAll(getUnicodeFromCharForReplacing("�"), "�");
        }
        if (query.contains(getUnicodeFromCharForReplacing("�"))) {
            query = query.replaceAll(getUnicodeFromCharForReplacing("�"), "�");
        }
        if (query.contains(getUnicodeFromCharForReplacing("�"))) {
            query = query.replaceAll(getUnicodeFromCharForReplacing("�"), "�");
        }
        if (query.contains(getUnicodeFromCharForReplacing("�"))) {
            query = query.replaceAll(getUnicodeFromCharForReplacing("�"), "�");
        }
        if (query.contains(getUnicodeFromCharForReplacing("�"))) {
            query = query.replaceAll(getUnicodeFromCharForReplacing("�"), "�");
        }

        return query;
    }

    public static String getUnicodedQuery(String query) {
        if (query.contains("�")) {
            query = query.replaceAll("�", "\\u00c3\\u201e");
        }
        if (query.contains("�")) {
            query = query.replaceAll("�", "\\u00c3\\u0153");
        }
        if (query.contains("�")) {
            query = query.replaceAll("�", "\\u00c3\\u2013");
        }
        if (query.contains("�")) {
            query = query.replaceAll("�", "\\u00c3\\u00a4");
        }
        if (query.contains("�")) {
            query = query.replaceAll("�", "\\u00c3\\u00bc");
        }
        if (query.contains("�")) {
            query = query.replaceAll("�", "\\u00c3\\u00b6");
        }
        if (query.contains("�")) {
            query = query.replaceAll("�", "\\u00c3\\u0178");
        }

        return query;
    }

    private static String getUnicodeFromChar(String c) {
        return "\\u00" + Integer.toHexString(c.toCharArray()[0]);
    }

    private static String getUnicodeFromCharForReplacing(String c) {
        return "u00" + Integer.toHexString(c.toCharArray()[0]);
    }

}
