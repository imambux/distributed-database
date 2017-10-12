package fdbs;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class Helper {

    public static InputStream convertToParsableQuery(String query) {
        InputStream parsableQuery = new ByteArrayInputStream(query.getBytes());
        return parsableQuery;
    }

}
