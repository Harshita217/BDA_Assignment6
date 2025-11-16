import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.*;

public class AmazonRedshiftQueries {

    private Connection con;

    // TODO: update with your cluster details
    private String url = "jdbc:redshift://default-workgroup.378173488523.us-east-1.redshift-serverless.amazonaws.com:5439/dev";
    private String uid = "admin";
    private String pw  = "Hello12345";

    public static void main(String[] args) {
        AmazonRedshiftQueries q = new AmazonRedshiftQueries();
        try {
            q.connect();
            System.out.println(q.resultSetToString(q.query1(), 20));
            System.out.println(q.resultSetToString(q.query2(), 20));
            System.out.println(q.resultSetToString(q.query3(), 20));
            q.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Connection connect() throws SQLException {
        System.out.println("Connecting...");
        con = DriverManager.getConnection(url, uid, pw);
        System.out.println("Connected!");
        return con;
    }

    public void drop() {
        System.out.println("Dropping all the tables");
  
        String[] tables = {
            "lineitem", "orders", "customer", "nation", "region", "supplier", "part", "partsupp"
        };

        Statement stmt = null;
        try {
            stmt = con.createStatement();
            for (String t : tables) {
                String sql = "DROP TABLE IF EXISTS dev." + t + " CASCADE;";
                try {
                    stmt.execute(sql);
                } catch (SQLException inner) {
                    try { stmt.execute("DROP TABLE IF EXISTS " + t + ";"); } catch (SQLException e2) {
                        System.err.println("Could not drop " + t + ": " + e2.getMessage());
                    }
                }
            }
        } catch (SQLException e) {
            System.err.println("Error in drop(): " + e.getMessage());
        } finally {
            if (stmt != null) try { stmt.close(); } catch (SQLException e) {}
        }
    }

        public void insert() throws SQLException
    {
        System.out.println("Loading TPC-H Data");
        String dataFolder = "data";
        File folder = new File(dataFolder);
        if (!folder.exists() || !folder.isDirectory()) {
            System.err.println("Data folder not found: " + dataFolder + ". If you use COPY from S3, create .sql files with COPY statements here.");
            return;
        }

        for (File f : files) {
            System.out.println("Executing data load file: " + f.getName());
            executeSqlFile(f);
        }
    }

        private void executeSqlFile(File file) {
        Statement stmt = null;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                // skip comments starting with --
                if (line.trim().startsWith("--")) continue;
                sb.append(line).append("\n");
            }
            String all = sb.toString();
            // naive split on semicolon for multiple statements
            String[] parts = all.split(";");
            stmt = con.createStatement();
            for (String p : parts) {
                String sql = p.trim();
                if (sql.length() == 0) continue;
                try {
                    stmt.execute(sql);
                } catch (SQLException e) {
                    System.err.println("Error executing statement (may be ok): " + e.getMessage());
                    System.err.println("Statement: " + (sql.length() > 200 ? sql.substring(0,200) + "..." : sql));
                }
            }
        } catch (Exception e) {
            System.err.println("Error reading/executing file " + file.getName() + ": " + e.getMessage());
        } finally {
            if (stmt != null) try { stmt.close(); } catch (SQLException e) {}
        }
    }


    public void close()
    {
        System.out.println("Closing database connection.");
        if (con != null) {
            try {
                con.close();
                System.out.println("Connection closed.");
            } catch (SQLException e) {
                System.err.println("Error while closing connection: " + e.getMessage());
            }
        }
    }
    
    public ResultSet query1() throws SQLException {
        System.out.println("------------------ QUERY 1 ------------------");
        String sql = """
                    SELECT o.o_orderkey,
               SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_sale,
               o.o_orderdate
        FROM dev.public.orders o
        JOIN dev.public.lineitem l ON o.o_orderkey = l.l_orderkey
        JOIN dev.public.customer c ON o.o_custkey = c.c_custkey
        JOIN dev.public.nation n ON c.c_nationkey = n.n_nationkey
        JOIN dev.public.region r ON n.n_regionkey = r.r_regionkey
        WHERE r.r_name = 'AMERICA'
        GROUP BY o.o_orderkey, o.o_orderdate
        ORDER BY o.o_orderdate DESC
        LIMIT 10;
        """;
        return con.prepareStatement(sql).executeQuery();
    }

    
    public ResultSet query2() throws SQLException {
        // find largest market segment
        System.out.println("------------------ QUERY 2 ------------------");
        String segSQL = """
            SELECT c_mktsegment
            FROM dev.public.customer
            GROUP BY c_mktsegment
            ORDER BY COUNT(*) DESC
            LIMIT 1;

        """;

        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(segSQL);
        rs.next();
        String largestSegment = rs.getString(1);
        rs.close();
        st.close();

        String sql = """
            SELECT c.c_custkey,
            SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_spent
            FROM dev.public.customer c
            JOIN dev.public.orders o ON c.c_custkey = o.o_custkey
            JOIN dev.public.lineitem l ON o.o_orderkey = l.l_orderkey
            JOIN dev.public.nation n ON c.c_nationkey = n.n_nationkey
            JOIN dev.public.region r ON n.n_regionkey = r.r_regionkey
            WHERE r.r_name <> 'EUROPE'
              AND c.c_mktsegment = ?
              AND o.o_orderpriority LIKE '%URGENT%'
              AND o.o_orderstatus <> 'F'
            GROUP BY c.c_custkey
            ORDER BY total_spent DESC;
        """;

        PreparedStatement pst = con.prepareStatement(sql);
        pst.setString(1, largestSegment);
        return pst.executeQuery();
    }

    
    public ResultSet query3() throws SQLException {
        System.out.println("------------------ QUERY 3 ------------------");
        String sql = """
            SELECT o.o_orderpriority,
            COUNT(*) AS lineitem_count
            FROM dev.public.orders o
            JOIN dev.public.lineitem l ON o.o_orderkey = l.l_orderkeyS
            WHERE o.o_orderdate >= DATE '1997-04-01'
              AND o.o_orderdate < DATE '2003-04-01'
            GROUP BY o.o_orderpriority
            ORDER BY o.o_orderpriority ASC;

        """;
        return con.prepareStatement(sql).executeQuery();
    }

    // ------------------ PRINT RESULTS ------------------
    public String resultSetToString(ResultSet rs, int maxrows) throws SQLException {
        StringBuilder out = new StringBuilder();
        ResultSetMetaData meta = rs.getMetaData();

        // print column headers
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            out.append(meta.getColumnName(i));
            if (i < meta.getColumnCount()) out.append(" | ");
        }
        out.append("\n");

        int count = 0;
        while (rs.next()) {
            if (count < maxrows) {
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    out.append(rs.getObject(i));
                    if (i < meta.getColumnCount()) out.append(" | ");
                }
                out.append("\n");
            }
            count++;
        }
        out.append("Rows returned: ").append(count).append("\n");
        return out.toString();
    }
    
}
