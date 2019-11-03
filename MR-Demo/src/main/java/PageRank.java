import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class PageRank extends Configured implements Tool {

  private static final Logger logger = LogManager.getLogger(PageRank.class);
  // set k
  private static final int k = 1000;

  /**
   * This is the class to construct graph.
   */
  public static class GraphMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(final Object key, final Text value, final Context context)
        throws IOException, InterruptedException {
      double initPR = 1.0 / (k * k);
      String val = null;
      for (int i = 1; i <= k * k; i++) {
        if (i % k == 0) {
          // the node who has no out edge, which is dangling page, all set to 0
          val = 0 + "," + initPR;
        } else {
          val = i + 1 + "," + initPR;
        }
        context.write(new Text(Integer.toString(i)), new Text(val));
      }
      val = "all|" + 0;
      context.write(new Text(Integer.toString(0)), new Text(val));
    }
  }

  

}