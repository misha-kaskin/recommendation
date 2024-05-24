import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Test {
    private static final Connection conn = Config.initConn();
    private static final Map<Integer, Map<Integer, Map<Integer, List<Integer>>>> trueMap = new HashMap<>();
    private static final Map<Integer, Map<Integer, Map<Integer, List<Integer>>>> fakeMap = new HashMap<>();
    private static final Map<Integer, List<Dto>> resMap = new HashMap<>();
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    public static void main(String[] args) throws SQLException {
        main(1);
        main2();
        main3();
        main4();
        main5();
        main6(21);
    }

    public static void main6(int cluster) throws SQLException {
        String sql = "SELECT t1.clusters, count(DISTINCT t2.order_id) \n" +
                "FROM (\n" +
                "\tSELECT user_id, clusters\n" +
                "\tFROM kmeans_summary ks\n" +
                "\tWHERE clusters = 21\n" +
                ") t1\n" +
                "JOIN orders t2 ON (t1.user_id = t2.user_id AND eval_set LIKE 'prior')\n" +
                "GROUP BY t1.clusters;";

        Connection conn1 = conn;
        PreparedStatement ps = conn1.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();

        while (resultSet.next()) {
            System.out.println(resultSet.getInt(2));
        }
    }

    public static void main(int k) throws SQLException {
        String sql = "SELECT t3.clusters, t3.user_id, t3.order_id, t4.product_id\n" +
                "FROM (\n" +
                "\tSELECT ks.clusters, t1.user_id, t1.order_id \n" +
                "\tFROM (\n" +
                "\t\tSELECT order_id, user_id\n" +
                "\t\tFROM orders\n" +
                "\t\tWHERE (\n" +
                "\t\t\teval_set LIKE 'prior' AND user_id IN (\n" +
                "\t\t\t\tSELECT user_id\n" +
                "\t\t\t\tFROM kmeans_summary\n" +
                "\t\t\t)\n" +
                "\t\t)\n" +
                "\t) t1\n" +
                "\tJOIN kmeans_summary ks ON (ks.user_id = t1.user_id)\n" +
                ") t3\n" +
                "JOIN order_products__prior t4 ON (t3.order_id = t4.order_id);";

        Connection conn1 = conn;
        PreparedStatement ps = conn1.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();

        while (resultSet.next()) {
            int clusterId = resultSet.getInt(1);
            int userId = resultSet.getInt(2);
            int orderId = resultSet.getInt(3);
            int productId = resultSet.getInt(4);
            trueMap.computeIfAbsent(clusterId, k1 -> new HashMap<>());
            trueMap.get(clusterId).computeIfAbsent(userId, k1 -> new HashMap<>());
            trueMap.get(clusterId).get(userId).computeIfAbsent(orderId, k1 -> new ArrayList<>());
            trueMap.get(clusterId).get(userId).get(orderId).add(productId);
            fakeMap.computeIfAbsent(clusterId, k1 -> new HashMap<>());
            fakeMap.get(clusterId).computeIfAbsent(userId, k1 -> new HashMap<>());
        }

        for (Integer clusterId : trueMap.keySet()) {
            for (Integer userId: trueMap.get(clusterId).keySet()) {
                for (Integer orderId : trueMap.get(clusterId).get(userId).keySet()) {
                    List<Integer> products = trueMap.get(clusterId).get(userId).get(orderId);
                    fakeMap.get(clusterId).get(userId).put(orderId, products.subList(0, Math.max(products.size() - k, 0)));
                }
            }
        }
    }

    public static void main2() throws SQLException {
        String sql = "SELECT order_id, count(product_id) counts \n" +
                "FROM order_products__prior op\n" +
                "GROUP BY order_id\n" +
                "ORDER BY counts DESC\n" +
                "LIMIT 100;";

        Connection conn1 = conn;
        PreparedStatement ps = conn1.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();

        while (resultSet.next()) {
            int i1 = resultSet.getInt(1);
            int i2 = resultSet.getInt(2);
            System.out.println(i1 + " " + i2);
        }
    }

    public static void main3() throws SQLException {
        String sql = "SELECT t.clusters, count(DISTINCT op.product_id) counts\n" +
                "FROM (\n" +
                "\tSELECT ks.clusters, o.order_id\n" +
                "\tFROM kmeans_summary ks\n" +
                "\tJOIN orders o ON (ks.user_id = o.user_id AND eval_set LIKE 'prior')\n" +
                ") t\n" +
                "JOIN order_products__prior op ON (op.order_id = t.order_id)\n" +
                "GROUP BY t.clusters\n" +
                "ORDER BY counts;";

        Connection conn1 = conn;
        PreparedStatement ps = conn1.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();

        while (resultSet.next()) {
            int i1 = resultSet.getInt(1);
            int i2 = resultSet.getInt(2);
            System.out.println(i1 + " " + i2);
        }
    }

    public static void main4() throws SQLException {
        for (int i = 0; i < 40; i++) {
            List<Dto> products = Getter.getProducts(i, 150);
            resMap.put(i, products);
        }
    }

    public static void main5() {
        int goal = 0;
        int total = 0;
        for (Integer clusterId : trueMap.keySet()) {
            List<Dto> topItems = resMap.get(clusterId);
            for (Integer userId: trueMap.get(clusterId).keySet()) {
                for (Integer orderId : trueMap.get(clusterId).get(userId).keySet()) {
                    List<Integer> trueProducts = trueMap.get(clusterId).get(userId).get(orderId);
                    List<Integer> fakeProducts = fakeMap.get(clusterId).get(userId).get(orderId);
                    int delta = trueProducts.size() - fakeProducts.size();
                    total += delta;

                    int counter = 0;
                    List<Integer> curList = new ArrayList<>();
                    for (Dto topItem : topItems) {
                        if (counter == delta) {
                            break;
                        }
                        if (!fakeProducts.contains(topItem.getId())) {
                            curList.add(topItem.getId());
                            counter++;
                        }
                    }

                    for (Integer cur : curList) {
                        if (trueProducts.contains(cur)) {
                            goal++;
                        }
                    }
                }
            }
        }
        double res = (double) goal / total;
        System.out.println(res * 100);
    }
}
