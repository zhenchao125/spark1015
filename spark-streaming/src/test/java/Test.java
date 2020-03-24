import java.io.IOException;

/**
 * @Author lzc
 * @Date 2020/3/24 8:52
 */
public class Test {
    public static void main(String[] args) throws IOException {
        /*BufferedReader in = new BufferedReader(new InputStreamReader( System.in, "utf8"));
        String a = in.readLine();
        System.out.println(a);*/

        String[] split = "".split("\\W+");
        System.out.println(split.length);

    }
}
