import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank extends Configured implements Tool {

  private static final Logger logger = LogManager.getLogger(PageRank.class);
  // set k
  private static final int k = 1000;

  /**
   * This is the class to construct graph.
   */
  public static class ConstructMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      System.out.println("----------------------------------------------------");
      String val;
      for (int i = 1; i <= k * k; i++) {
        System.out.println(k);
        if (i % k == 0) {
          // the node who has no out edge, which is dangling page, all set to 0 as out
          val = 0 + "," + 1.0 / (k * k);
        } else {
          int j = i + 1;
          val = j + "," + 1.0 / (k * k);
        }
        context.write(new Text(Integer.toString(i)), new Text(val));
      }
      // this dummy should output to all other nodes
      val = "dummy" + "," + 0;
      context.write(new Text(Integer.toString(0)), new Text(val));
    }
  }

  /**
   * Map process on vertex n which contains current page rank and its adj list.
   */
  public static class GraphMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] strings = value.toString().split("\t");
      // emit the graph structure
      context.write(new Text(strings[0]), new Text(strings[1]));
      String[] vals = strings[1].split(",");

      double pr = Double.parseDouble(vals[1]);
      if (vals[0].equals("dummy")) {
        // send to all other nodes
        for (int i = 1; i <= k * k; i++) {
          context.write(new Text(Integer.toString(i)), new Text(Double.toString(pr / (k * k))));
        }
      } else {
        // emit to all node in adj list, in this case just one.
        context.write(new Text(vals[0]), new Text(Double.toString(pr)));
      }

    }
  }

  // same as the module pseudo code
  public static class GraphReducer extends Reducer<Text, Text, Object, Object> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      String adjNode = null;
      double sum = 0.0;
      for (Text val : values) {
        String[] strings = val.toString().split(",");
        // when this is a graph structure
        if (strings.length > 1) {
          // get the adjust node, abandon strings[1] which is the old pr val
          adjNode = strings[0];
        } else {
          sum += Double.parseDouble(val.toString());
        }
      }
      // update the pr value and keep the graph
      sum = 0.15 / k * k + 0.85 * sum;
      // restore the graph structure with adjNode amd the new pr value
      context.write(key, new Text(adjNode + "," + sum));
    }
  }

  /**
   * Extract the dummy 0 pr and send to others since the final result should not contain 0 node.
   */
  public static class CombineMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] strings = value.toString().split("\t");
      String[] vals = strings[1].split(",");
      double pr = Double.parseDouble(vals[1]);
      if (strings[0].equals(Integer.toString(0))) {
        for (int i = 1; i <= k * k; i++) {
          // dummy gives all pr away
          context.write(new Text(Integer.toString(i)), new Text(Double.toString(pr / (k * k))));
        }
      } else {
        // map the original pr
        context.write(new Text(strings[0]), new Text(Double.toString(pr)));
      }
    }
  }

  /**
   * Sum the pr gave out by dummy. Values should contain just two objects.
   */
  public static class CombineReducer extends Reducer<Text, Text, Object, Object> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      double sum = 0.0;
      for (Text val : values) {
        sum += Double.parseDouble(val.toString());
      }
      context.write(key, new Text(Double.toString(sum)));
    }
  }

  @Override
  public int run(final String[] args) throws Exception {
    final Configuration conf = getConf();
    final Job job_Graph = Job.getInstance(conf, "Construct Graph");
    job_Graph.setJarByClass(PageRank.class);
    job_Graph.setInputFormatClass(NLineInputFormat.class);
    job_Graph.setMapperClass(ConstructMapper.class);
    job_Graph.setOutputKeyClass(Text.class);
    job_Graph.setOutputValueClass(Text.class);
    NLineInputFormat.addInputPath(job_Graph, new Path(args[0]));
    FileOutputFormat.setOutputPath(job_Graph, new Path(args[1]));
    return job_Graph.waitForCompletion(true) ? 0 : 1;
  }



  public static void main(final String[] args) {
    if (args.length != 2) {
      throw new Error("Arguments required:\n<input-dir> <output-dir>");
    }
    try {
      ToolRunner.run(new PageRank(), args);
    } catch (final Exception e) {
      logger.error("", e);
    }
  }


}