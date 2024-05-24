import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class DaoUtil {

    private static final Connection conn = Config.initConn();

    public static void initAisles() throws IOException, SQLException {
        Statement st = conn.createStatement();
        String sqlAisles = "CREATE TABLE aisles (aisle_id int, aisle text);";
        st.execute(sqlAisles);

        byte[] bytes = Files.readAllBytes(Path.of("src/main/resources/aisles.csv"));
        String data = new String(bytes);
        String[] dates = data.split("\n");
        System.out.println(dates.length);
        StringBuilder builder = new StringBuilder("INSERT INTO aisles (aisle_id, aisle)\n");
        builder.append("VALUES\n");
        for (int i = 1; i < dates.length - 1; i++) {
            String str = "(" + dates[i].split(",")[0] + ", '" + dates[i].split(",")[1] + "'),\n";
            builder.append(str);
        }
        String str = "(" + dates[dates.length - 1].split(",")[0] + ", '" + dates[dates.length - 1].split(",")[1] + "');";
        builder.append(str);
        st.execute(builder.toString());

        st.close();
    }

    public static void initDepartments() throws IOException, SQLException {
        Statement st = conn.createStatement();
        String sqlAisles = "CREATE TABLE departments (department_id int, department text);";
        st.execute(sqlAisles);

        byte[] bytes = Files.readAllBytes(Path.of("src/main/resources/departments.csv"));
        String data = new String(bytes);
        String[] dates = data.split("\n");
        System.out.println(dates.length);
        StringBuilder builder = new StringBuilder("INSERT INTO departments (department_id, department)\n");
        builder.append("VALUES\n");
        for (int i = 1; i < dates.length - 1; i++) {
            String str = "(" + dates[i].split(",")[0] + ", '" + dates[i].split(",")[1] + "'),\n";
            builder.append(str);
        }
        String str = "(" + dates[dates.length - 1].split(",")[0] + ", '" + dates[dates.length - 1].split(",")[1] + "');";
        builder.append(str);
        st.execute(builder.toString());

        st.close();
    }

    public static void initOrderProductsPrior() throws SQLException, IOException {
        Statement st = conn.createStatement();
        String sqlAisles = "CREATE TABLE order_products__prior (order_id int, product_id int, add_to_cart_order int, reordered int);";
        st.execute(sqlAisles);

        byte[] bytes = Files.readAllBytes(Path.of("src/main/resources/order_products__prior.csv"));
        String data = new String(bytes);
        String[] dates = data.split("\n");
        System.out.println(dates.length);

        int coef = 1_000_000;
        int c = dates.length / coef;
        int i;
        for (int j = 0; j < c + 1; j++) {
            StringBuilder builder = new StringBuilder("INSERT INTO order_products__prior (order_id, product_id, add_to_cart_order, reordered)\n");
            builder.append("VALUES\n");
            for (i = 1; i < coef && j * coef + i < dates.length - 1; i++) {
                String str = "(" + dates[j * coef + i].split(",")[0] +
                        ", "
                        + dates[j * coef + i].split(",")[1]
                        + ", "
                        + dates[j * coef + i].split(",")[2]
                        + ", "
                        + dates[j * coef + i].split(",")[3]
                        + "),\n";
                builder.append(str);
            }

            String str = "(" + dates[j * coef + i].split(",")[0] +
                    ", "
                    + dates[j * coef + i].split(",")[1]
                    + ", "
                    + dates[j * coef + i].split(",")[2]
                    + ", "
                    + dates[j * coef + i].split(",")[3]
                    + ");";
            builder.append(str);
            st.execute(builder.toString());
        }

        st.close();
    }

    public static void initOrderProductsTrain() throws SQLException, IOException {
        Statement st = conn.createStatement();
        String sqlAisles = "CREATE TABLE order_products__train (order_id int, product_id int, add_to_cart_order int, reordered int);";
        st.execute(sqlAisles);

        byte[] bytes = Files.readAllBytes(Path.of("src/main/resources/order_products__train.csv"));
        String data = new String(bytes);
        String[] dates = data.split("\n");
        System.out.println(dates.length);

        int coeff = 1_000_000;
        int c = dates.length / coeff;
        int i;
        for (int j = 0; j < c + 1; j++) {
            StringBuilder builder = new StringBuilder("INSERT INTO order_products__train (order_id, product_id, add_to_cart_order, reordered)\n");
            builder.append("VALUES\n");
            for (i = 1; i < coeff && j * coeff + i < dates.length - 1; i++) {
                String str = "(" + dates[j * coeff + i].split(",")[0] +
                        ", "
                        + dates[j * coeff + i].split(",")[1]
                        + ", "
                        + dates[j * coeff + i].split(",")[2]
                        + ", "
                        + dates[j * coeff + i].split(",")[3]
                        + "),\n";
                builder.append(str);
            }

            String str = "(" + dates[j * coeff + i].split(",")[0] +
                    ", "
                    + dates[j * coeff + i].split(",")[1]
                    + ", "
                    + dates[j * coeff + i].split(",")[2]
                    + ", "
                    + dates[j * coeff + i].split(",")[3]
                    + ");";
            builder.append(str);
            st.execute(builder.toString());
        }

        st.close();
    }

    public static void initOrders() throws SQLException, IOException {
        Statement st = conn.createStatement();
        String sqlAisles = "CREATE TABLE orders (order_id int, user_id int, eval_set text, order_number int, order_dow int, order_hour_of_day int, days_since_prior_order real);";
        st.execute(sqlAisles);

        byte[] bytes = Files.readAllBytes(Path.of("src/main/resources/orders.csv"));
        String data = new String(bytes);
        String[] dates = data.split("\n");
        System.out.println(dates.length);

        int coeff = 1_000_000;
        int c = dates.length / coeff;
        int i;
        for (int j = 0; j < c + 1; j++) {
            StringBuilder builder = new StringBuilder("INSERT INTO orders (order_id, user_id, eval_set, order_number, order_dow, order_hour_of_day, days_since_prior_order)\n");
            builder.append("VALUES\n");

            for (i = 1; i < coeff && j * coeff + i < dates.length - 1; i++) {
                String[] strings = dates[j * coeff + i].split(",");
                String lastString;
                if (strings.length == 7) {
                    lastString = strings[6];
                } else {
                    lastString = String.valueOf(0);
                }
                String str = "(" + dates[j * coeff + i].split(",")[0] +
                        ", "
                        + dates[j * coeff + i].split(",")[1]
                        + ", '"
                        + dates[j * coeff + i].split(",")[2]
                        + "', "
                        + dates[j * coeff + i].split(",")[3]
                        + ", "
                        + dates[j * coeff + i].split(",")[4]
                        + ", "
                        + dates[j * coeff + i].split(",")[5]
                        + ", "
                        + lastString
                        + "),\n";
                builder.append(str);
            }

            String[] strings = dates[j * coeff + i].split(",");
            String lastString;
            if (strings.length == 7) {
                lastString = strings[6];
            } else {
                lastString = String.valueOf(0);
            }
            String str = "(" + dates[j * coeff + i].split(",")[0] +
                    ", "
                    + dates[j * coeff + i].split(",")[1]
                    + ", '"
                    + dates[j * coeff + i].split(",")[2]
                    + "', "
                    + dates[j * coeff + i].split(",")[3]
                    + ", "
                    + dates[j * coeff + i].split(",")[4]
                    + ", "
                    + dates[j * coeff + i].split(",")[5]
                    + ", "
                    + lastString
                    + ");";
            builder.append(str);
            st.execute(builder.toString());
        }

        st.close();
    }

    public static void initProducts() throws SQLException, IOException {
        Statement st = conn.createStatement();
        String sqlAisles = "CREATE TABLE products (product_id int, product_name text, aisle_id int, department_id int);";
//        st.execute(sqlAisles);

        byte[] bytes = Files.readAllBytes(Path.of("src/main/resources/products.csv"));
        String data = new String(bytes);
        String[] dates = data.split("\n");
        System.out.println(dates.length);
        StringBuilder builder = new StringBuilder("INSERT INTO products (product_id, product_name, aisle_id, department_id)\n");
        builder.append("VALUES\n");
        String regex = ",";

        for (int i = 1; i < dates.length - 1; i++) {
            String[] strings = dates[i].split(regex);
            String title = strings[1];
            for (int j = 2; j < strings.length - 2; j++) {
                title +=  "," + strings[j];
            }
            title = title.replaceAll("'", "");

            String str = "("
                    + strings[0]
                    + ", '"
                    + title
                    + "', "
                    + strings[strings.length - 2]
                    +", "
                    + strings[strings.length - 1]
                    + "),\n";
            builder.append(str);
        }
        String str = "("
                + dates[dates.length - 1].split(regex)[0]
                + ", '"
                + dates[dates.length - 1].split(regex)[1]
                + "', "
                + dates[dates.length - 1].split(regex)[2]
                +", "
                + dates[dates.length - 1].split(regex)[3]
                + ");";

        builder.append(str);
        st.execute(builder.toString());

        st.close();
    }

    public static void initSampleSubmission() throws SQLException, IOException {
        Statement st = conn.createStatement();
        String sqlAisles = "CREATE TABLE sample_submission (order_id int, products text);";
        st.execute(sqlAisles);

        byte[] bytes = Files.readAllBytes(Path.of("src/main/resources/sample_submission.csv"));
        String data = new String(bytes);
        String[] dates = data.split("\n");
        System.out.println(dates.length);
        StringBuilder builder = new StringBuilder("INSERT INTO sample_submission (order_id, products)\n");
        builder.append("VALUES\n");
        for (int i = 1; i < dates.length - 1; i++) {
            String str = "(" + dates[i].split(",")[0] + ", '" + dates[i].split(",")[1] + "'),\n";
            builder.append(str);
        }
        String str = "(" + dates[dates.length - 1].split(",")[0] + ", '" + dates[dates.length - 1].split(",")[1] + "');";
        builder.append(str);
        st.execute(builder.toString());

        st.close();
    }

    public static void initKMeansSummary() throws SQLException, IOException {
        Statement st = conn.createStatement();
        String sqlAisles = "CREATE TABLE kmeans_summary (id int, num_0 real, num_1 real, user_id int, x real, y real, clusters int);";
        st.execute(sqlAisles);

        byte[] bytes = Files.readAllBytes(Path.of("src/main/resources/kmeans_summary.csv"));
        String data = new String(bytes);
        String[] dates = data.split("\n");
        System.out.println(dates.length);
        StringBuilder builder = new StringBuilder("INSERT INTO kmeans_summary (id, num_0, num_1, user_id, x, y, clusters)\n");
        builder.append("VALUES\n");
        for (int i = 1; i < dates.length - 1; i++) {
            String str = "("
                    + dates[i].split(",")[0]
                    + ", "
                    + dates[i].split(",")[1]
                    + ", "
                    + dates[i].split(",")[2]
                    + ", "
                    + dates[i].split(",")[3]
                    + ", "
                    + dates[i].split(",")[4]
                    + ", "
                    + dates[i].split(",")[5]
                    + ", "
                    + dates[i].split(",")[6]
                    + "),\n";
            builder.append(str);
        }
        String str = "("
                + dates[dates.length - 1].split(",")[0]
                + ", "
                + dates[dates.length - 1].split(",")[1]
                + ", "
                + dates[dates.length - 1].split(",")[2]
                + ", "
                + dates[dates.length - 1].split(",")[3]
                + ", "
                + dates[dates.length - 1].split(",")[4]
                + ", "
                + dates[dates.length - 1].split(",")[5]
                + ", "
                + dates[dates.length - 1].split(",")[6]
                + ");";

        builder.append(str);
        st.execute(builder.toString());

        st.close();
    }

    public static void main(String[] args) throws SQLException, IOException {
//        DaoUtil.initAisles();
//        DaoUtil.initDepartments();
//        DaoUtil.initOrderProductsPrior();
//        DaoUtil.initOrderProductsTrain();
//        DaoUtil.initOrders();
//        DaoUtil.initProducts();
//        DaoUtil.initSampleSubmission();
//        DaoUtil.initKMeansSummary();
    }
}
