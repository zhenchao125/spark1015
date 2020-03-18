import java.text.DecimalFormat

/**
 * Author atguigu
 * Date 2020/3/18 16:55
 */
object Test1 {
    def main(args: Array[String]): Unit = {
        // SimpleDataFormat
        val f = new DecimalFormat("0000.00")
        println(f.format(math.Pi))
        println(f.format(1))
        println(f.format(100))
        println(f.format(2222))
        println(f.format(444444))
    }
}
