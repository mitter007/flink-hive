package csdn01;
 
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
 
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
 
//使用并行度为1的source
public class MyJsonParalleSource implements SourceFunction<String> {//1
 
    //private long count = 1L;
    private boolean isRunning = true;
    List<String> ipList = Arrays.asList("192.168.25.3","192.168.25.4","192.168.25.5","192.168.25.6","192.168.25.7","192.168.25.8");
    /**
     * 主要的方法
     * 启动一个source
     * 大部分情况下，都需要在这个run方法中实现一个循环，这样就可以循环产生数据了
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while(isRunning){
            // ip信息描述
            int i = new Random().nextInt(5);
            JSONObject jsonObject = JSONUtil.createObj()
                    .set("ip", ipList.get(i))
                    .set("msg", ipList.get(i) + "接进来，请接听对应信息")
                    .set("ts", System.currentTimeMillis());
            String book = jsonObject.toString();
            ctx.collect(book);
            System.out.println("-----------------"+book);
            //每5秒产生一条数据
            Thread.sleep(5000);
        }
    }
    //取消一个cancel的时候会调用的方法
    @Override
    public void cancel() {
        isRunning = false;
    }
}