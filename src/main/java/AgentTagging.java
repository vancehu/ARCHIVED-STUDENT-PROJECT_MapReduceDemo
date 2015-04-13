import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jettison.json.JSONArray;
import org.json.JSONObject;

import java.awt.geom.Line2D;
import java.awt.geom.Rectangle2D;
import java.io.IOException;

/**
 * Created by Vance on 4/13/2015.
 */
public class AgentTagging {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        //(key, seg) -> (seg, grid)
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //seg format: {"segId": 1, "x0": ..., "x1": ..., "y0": ..., "y1": ..., "adjacent": [{"segId", 1}, {"segId", 2}]
            JSONObject seg = new JSONObject(value.toString());
            for (int i = 0; i < maxRow; i++) {
                for (int j = 0; j < maxCol; j++) {
                    Line2D line = new Line2D.Float(seg.getLong("x0"), seg.getLong("y0"), seg.getLong("x1"), seg.getLong
                            ("y1"));
                    Rectangle2D rect = new Rectangle2D.Float(j * gridWidth, i * gridHeight, gridWidth, gridHeight);
                    if (rect.intersectsLine(line) || (rect.contains(line.getP1()) && rect.contains(line.getP2()))) {
                        //intersect or contain
                        JSONObject grid = new JSONObject();
                        grid.put("i", i);
                        grid.put("j", j);
                        context.write(new Text(grid.toString()), new Text(seg.toString()));
                    }
                }
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            JSONArray arr = new JSONArray();
            for (Text value : values) {
                arr.put(new JSONObject(value.toString()));
            }
            JSONObject segments = new JSONObject();
            segments.put("segments", arr);
            context.write(key, new Text(segments.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = execute(new Path(args[0]), new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static Job execute(Path input, Path output) throws IOException {
        final Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SegmentIndexing");
        job.setJarByClass(SegmentIndexing.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }
}
