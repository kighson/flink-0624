import org.apache.flink.util.MathUtils;

/**
 * @author Hefei
 * @description
 * @project_name flink-0624
 * @package_name PACKAGE_NAME
 * @since 2021/11/8 15:40
 */
public class TestMurmurHash {
    public static void main(String[] args) {


        String c = "偶数";
        String d = "奇数";

        System.out.println(MathUtils.murmurHash(1) % 128);
        System.out.println(MathUtils.murmurHash(0) % 128);
        System.out.println(MathUtils.murmurHash(c.hashCode()) % 128);
        System.out.println(MathUtils.murmurHash(d.hashCode()) % 128);

    }
}