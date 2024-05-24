import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static java.util.stream.Collectors.toList;

public class Getter {
    private static final Connection conn = Config.initConn();
    private static final Map<Integer, List<String>> bucket = new HashMap<>();
    private static final Map<Integer, List<Dto>> recommendationCache = new HashMap<>();
    private static final Map<Integer, Integer> userCluster = new HashMap<>();
    private static final List<Integer> users = new ArrayList<>();
    private static int user = 37;
    private static final Gson gson = new GsonBuilder()
            .setPrettyPrinting()
            .create();

    public static void main(String[] args) throws SQLException, IOException {
        Scanner scanner = new Scanner(System.in);
        getUsersInit();
        initUserCluster();
//        Thread thread = new Thread(() -> System.out.println("\u001B[31m" + "GOODBYE DIMA!!!"));
//        Thread thread = new Thread(() -> {
//            while (true);
//        });
//        Runtime.getRuntime().addShutdownHook(thread);
        System.out.println(getUsers2(0, 10));

        while (true) {
            String str = scanner.nextLine();
            String[] strs = str.split("\\s");
            switch (strs[0]) {
                case "getusers": {
                    if (strs.length < 3) {
                        System.out.println(getUsers2(0, 15));
                    } else {
                        try {
                            int offset = Integer.parseInt(strs[1]);
                            int limit = Integer.parseInt(strs[2]);
                            System.out.println(getUsers2(offset, limit));
                        } catch (Exception e) {
                            System.out.println("Incorrect format..............");
                            break;
                        }
                    }
                    break;
                }
                case "user": {
                    if (strs.length < 2) {
                        user = users.get(0);
                    } else {
                        int id = -1;
                        try {
                            id = Integer.parseInt(strs[1]);
                            if (!users.contains(id)) {
                                break;
                            } else {
                                user = id;
                            }
                        } catch (Exception e) {
                            System.out.println("Incorrect format..............");
                            break;
                        }
                        System.out.println("Hello Dima!!!");
                        user = users.get(id);
                    }
                    break;
                }
                case "cluster": {
                    if (!userCluster.containsKey(user)) {
                        System.out.println("Not found user..................");
                        break;
                    } else {
                        System.out.println(userCluster.get(user));
                    }
                    break;
                }
                case "getorders": {
                    if (!userCluster.containsKey(user)) {
                        System.out.println("Not found user..................");
                        break;
                    } else {
                        int parameter = 100;
                        if (strs.length == 2) {
                            try {
                                parameter = Integer.parseInt(strs[1]);
                            } catch (Exception e) {
                                System.out.println("Incorrect format..............");
                                break;
                            }
                        }
                        System.out.println(gson.toJson(getOrder(user, parameter)));
                    }
                    break;
                }
                case "add": {
                    if (strs.length < 2) {
                        System.out.println("Incorrect format..............");
                        break;
                    } else {
                        int param;
                        try {
                            param = Integer.parseInt(strs[1]);
                        } catch (Exception e) {
                            System.out.println("Incorrect format..............");
                            break;
                        }
                        bucket.computeIfAbsent(user, v -> new ArrayList<>());
                        Optional<String> optional = getItem(param);
                        optional.ifPresentOrElse(
                                el -> bucket.get(user).add(el),
                                () -> System.out.println("Item not present.............")
                        );
                    }
                    break;
                }
                case "rm": {
                    if (strs.length < 2) {
                        System.out.println("Incorrect format..............");
                        break;
                    } else {
                        String item;
                        int param;
                        try {
                            param = Integer.parseInt(strs[1]);
                            Optional<String> optional = getItem(param);
                            item = optional.orElseThrow();
                        } catch (Exception e) {
                            System.out.println("Incorrect format..............");
                            break;
                        }
                        Optional<List<String>> strings = Optional.ofNullable(bucket.get(user));
                        strings.ifPresentOrElse(
                                el -> {
                                    boolean isRm = el.remove(item);
                                    if (!isRm) {
                                        System.out.println("Item not contains in bucket...............");
                                    }
                                },
                                () -> System.out.println("Item not contains in bucket...............")
                        );
                    }
                    break;
                }
                case "rec": {
                    if (strs.length != 2) {
                        System.out.println("Incorrect format..............");
                        break;
                    } else {
                        int param;
                        try {
                            param = Integer.parseInt(strs[1]);
                            List<Dto> list = getProducts(userCluster.get(user), 100);
                            bucket.computeIfAbsent(user, k -> new ArrayList<>());
                            List<String> userItems = bucket.get(user);

                            List<String> subList = list.stream()
                                    .map(Dto::getName)
                                    .filter(el -> !userItems.contains(el))
                                    .collect(toList())
                                    .subList(0, param);

                            System.out.println(gson.toJson(subList));
                        } catch (Exception e) {
                            System.out.println("Incorrect format..............");
                            break;
                        }
                    }
                    break;
                }
                case "top": {
                    if (strs.length != 2) {
                        System.out.println("Incorrect format..............");
                        break;
                    } else {
                        int param;
                        try {
                            param = Integer.parseInt(strs[1]);
                            List<Dto> list = getProducts(userCluster.get(user), 100).subList(0, param);
                            System.out.println(gson.toJson(list));
                        } catch (Exception e) {
                            System.out.println("Incorrect format..............");
                            break;
                        }
                    }
                    break;
                }
                case "cluster1" : {
                    if (strs.length != 2) {
                        System.out.println("Incorrect format..............");
                        break;
                    } else {
                        int param;
                        try {
                            param = Integer.parseInt(strs[1]);
                            List<Integer> list = getClusters1(param);
                            System.out.println(gson.toJson(list));
                        } catch (Exception e) {
                            System.out.println("Incorrect format..............");
                            break;
                        }
                    }
                    break;
                }
                case "bucket": {
                    Optional<List<String>> strings = Optional.ofNullable(bucket.get(user));
                    strings.ifPresentOrElse(
                            el -> System.out.println(gson.toJson(el)),
                            () -> System.out.println("Empty bucket")
                    );
                    break;
                }
                case "exit": {
                    return;
                }
                default: {
                    System.out.println("Unknown command....................");
                }
            }
        }
    }

    public static Optional<String> getItem(int itemId) throws SQLException {
        String sql = "SELECT product_name FROM products WHERE product_id = ? LIMIT 1;";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setInt(1, itemId);
        ResultSet resultSet = ps.executeQuery();
        String res = null;
        while (resultSet.next()) {
            res = resultSet.getString(1);
        }
        return Optional.ofNullable(res);
    }

    public static Map<Integer, List<String>> getOrder(int userId, int limit) throws SQLException {
        String sql = "SELECT o2.order_id, p.product_name, count(p.product_name) count\n" +
                "FROM (\t\n" +
                "\tSELECT o.order_id, opt.product_id\n" +
                "\tFROM orders o\n" +
                "\tJOIN order_products__prior opt ON (o.order_id = opt.order_id AND o.user_id = ? AND o.eval_set LIKE 'prior')\n" +
                ") o2\n" +
                "JOIN products p ON (o2.product_id = p.product_id)\n" +
                "GROUP BY o2.order_id, p.product_name\n" +
                "LIMIT ?;";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setInt(1, userId);
        ps.setInt(2, limit);
        Map<Integer, List<String>> map = new HashMap<>();
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            int id = rs.getInt(1);
            String str = rs.getString(2).replaceAll("\\u0026", "");
            int count = rs.getInt(3);
            String value = str + " x " + count;
            map.computeIfAbsent(id, v -> new ArrayList<>());
            map.get(id).add(value);
        }
        return map;
    }

    public static List<Integer> getUsers2(int offset, int limit) {
        return users.subList(offset, offset + limit);
    }

    public static List<Integer> getClusters1(int cluster) throws SQLException {
        String sql = "SELECT user_id\n" +
                "FROM kmeans_summary\n" +
                "WHERE clusters = ?\n" +
                "LIMIT 5;\n";

        Connection conn1 = conn;
        PreparedStatement ps = conn1.prepareStatement(sql);
        ps.setInt(1, cluster);
        ResultSet resultSet = ps.executeQuery();
        List<Integer> list = new ArrayList<>();
        while (resultSet.next()) {
            list.add(resultSet.getInt(1));
        }
        return list;
    }

    public static void initUserCluster() throws SQLException {
        String sql = "SELECT user_id, clusters FROM kmeans_summary;";
        Connection conn1 = conn;
        PreparedStatement ps = conn1.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            int i1 = resultSet.getInt(1);
            int i2 = resultSet.getInt(2);
            userCluster.put(i1, i2);
        }
    }

    public static void getUsersInit() throws SQLException {
        String sql = "SELECT user_id\n" +
                "FROM kmeans_summary;";
        Connection conn1 = conn;
        PreparedStatement ps = conn1.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            int i1 = resultSet.getInt(1);
            users.add(i1);
        }
    }

    public static void getStat() throws SQLException {
        String sql = "SELECT t1.clusters, t1.users, t1.products, t1.orders, t2.order_dow, t2.order_hour_of_day, t2.days_since_prior_order\n" +
                "FROM (\n" +
                "\tSELECT t.clusters, count(t.user_id) users, avg(t.products) products, avg(t.orders) orders\n" +
                "\tFROM (\n" +
                "\t\tSELECT l1.clusters, l1.user_id, count(r1.product_id) products, count(DISTINCT l1.order_id) orders\n" +
                "\t\tFROM (\n" +
                "\t\t\tSELECT l.clusters, l.user_id, r.order_id\n" +
                "\t\t\tFROM kmeans_summary l\n" +
                "\t\t\tJOIN orders r ON (l.user_id = r.user_id AND r.eval_set LIKE 'prior')\n" +
                "\t\t) l1\n" +
                "\t\tJOIN order_products__prior r1 ON (l1.order_id = r1.order_id)\n" +
                "\t\tGROUP BY l1.clusters, l1.user_id\n" +
                "\t) t\n" +
                "\tGROUP BY clusters\n" +
                ") t1\n" +
                "JOIN (\n" +
                "\tSELECT k.clusters, avg(o.order_dow) order_dow, avg(o.order_hour_of_day) order_hour_of_day, avg(o.days_since_prior_order) days_since_prior_order\n" +
                "\tFROM kmeans_summary k \n" +
                "\tJOIN orders o ON (k.user_id = o.user_id)\n" +
                "\tGROUP BY k.clusters\n" +
                ") t2 ON (t1.clusters = t2.clusters)\n" +
                "ORDER BY users DESC;";

        Connection conn1 = conn;
        PreparedStatement ps = conn1.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        System.out.println("clusters\tusers\tproducts\torders\tdow\thours\tday_since");
        while (resultSet.next()) {
            int i1 = resultSet.getInt(1);
            int i2 = resultSet.getInt(2);
            double d3 = Math.round(resultSet.getDouble(3) * 1000);
            d3 /= 1000;
            double d4 = Math.round(resultSet.getDouble(4) * 1000);
            d4 /= 1000;
            double d5 = Math.round(resultSet.getDouble(5) * 1000);
            d5 = d5 / 1000;
            double d6 = Math.round(resultSet.getDouble(6) * 1000);
            d6 = d6 / 1000;
            double d7 = Math.round(resultSet.getDouble(7) * 1000);
            d7 = d7 / 1000;

            System.out.println(i1 + "\t" + i2 + "\t" + d3 + "\t" + d4 + "\t" + d5 + "\t" + d6 + "\t" + d7);
        }
    }

    public static List<Dto> getProducts(int cluster, int count) throws SQLException {
        if (recommendationCache.containsKey(cluster)) {
            return recommendationCache.get(cluster);
        }

        String sql3 = "SELECT l.product_id, r.product_name, l.counts\n" +
                "FROM (\n" +
                "\tSELECT product_id, count(order_id) counts\n" +
                "\tFROM order_products__prior\n" +
                "\tWHERE order_id IN (\n" +
                "\t\tSELECT order_id \n" +
                "\t\tFROM orders\n" +
                "\t\tWHERE user_id IN (\n" +
                "\t\t\tSELECT user_id \n" +
                "\t\t\tFROM kmeans_summary\n" +
                "\t\t\tWHERE clusters = ?\n" +
                "\t\t)\n" +
                "\t)\n" +
                "\tGROUP BY product_id\n" +
                "\tORDER BY counts DESC\n" +
                "\tLIMIT ?\n" +
                ") l\n" +
                "JOIN products r ON (l.product_id = r.product_id)\n" +
                "ORDER BY l.counts DESC;";

        Connection conn1 = conn;
        PreparedStatement ps = conn1.prepareStatement(sql3);
        ps.setInt(1, cluster);
        ps.setInt(2, count);
        ResultSet resultSet = ps.executeQuery();
        List<Dto> list = new ArrayList<>();

        while (resultSet.next()) {
            int i1 = resultSet.getInt(1);
            String string = resultSet.getString(2);
            int i2 = resultSet.getInt(3);
            String res = i1 + " " + string + " " + i2;
            list.add(Dto.builder().id(i1).name(string).count(i2).build());
        }
        recommendationCache.put(cluster, list);
        return list;
    }
}
