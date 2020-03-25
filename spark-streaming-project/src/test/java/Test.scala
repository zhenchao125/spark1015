/**
 * Author atguigu
 * Date 2020/3/25 9:39
 */
object Test {
    def main(args: Array[String]): Unit = {
        val it = Array(1,2,3).toIterator
    
        println(it.hasNext)
        println(it.isEmpty)
        println(it.toList)
    }
}
