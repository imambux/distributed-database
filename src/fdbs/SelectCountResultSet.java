package fdbs;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class SelectCountResultSet extends AbstractResultSet {
    private List<ResultSet> list;

    public SelectCountResultSet(List<ResultSet> list) {
        super(list);
        this.list = list;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        int i = 0;
        for (ResultSet rs : list) {
            i += Integer.parseInt(rs.getString(1));
        }
        return i;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        int i = 0;
        for (ResultSet rs : list) {
            i += Integer.parseInt(rs.getString(1));
        }
        return i + "";
    }

}
