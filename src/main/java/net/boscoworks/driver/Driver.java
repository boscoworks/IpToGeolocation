package net.boscoworks.driver;

import net.boscoworks.mapreduce.IpToGeolocationMapper;
import net.boscoworks.mapreduce.IpToGeolocationReducer;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	private static final String JAR_NAME = "IpToGeolocation.jar";

	@Override
	public int run(String[] args) throws Exception {
		// 引数の取得
		Configuration conf = getConf();
		String[] remaining_args = new GenericOptionsParser(conf, args).getRemainingArgs();

		// Jobの設定
		Job job = new Job();
		job.setJarByClass(Driver.class);

		// in_path の設定
		String[] in_pathes = new String[remaining_args.length - 1];
		for (int count = 0; count < remaining_args.length - 1; count++) {
			String in_path = remaining_args[count];
			in_pathes[count] = in_path;
			TextInputFormat.addInputPath(job, new Path(in_path));
		}

		// out_path の設定
		String out_path = remaining_args[remaining_args.length - 1];
		TextOutputFormat.setOutputPath(job, new Path(out_path));

		// Reducer数の設定
		job.setNumReduceTasks(65536);

		// Mapperの設定
		job.setMapperClass(IpToGeolocationMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Reducerの設定
		job.setReducerClass(IpToGeolocationReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// IOの設定
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Jobの名前の設定
		String job_name = JAR_NAME;
		for (int count = 0; count < in_pathes.length; count++) {
			job_name += " " + in_pathes[count];
		}
		job_name += " " + out_path;
		job.setJobName(job_name);

		job.getConfiguration().setBoolean("mapred.used.genericoptionsparser", true);
		if (job.waitForCompletion(true)) {
			return 0;
		} else {
			return 1;
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length >= 2) {
			int response = ToolRunner.run(new Configuration(), new Driver(), args);
			System.exit(response);
		} else {
			System.out.println(
					"ジョブを実行するためには以下のように入力してください\n" +
					"\thadoop jar " + JAR_NAME + " 引数\n" +
					"\t\t第1引数：入力ファイル（1個以上のHDFSフォルダパスを指定、スペース区切り）\n" +
					"\t\t第2引数：出力ファイル（1つのHDFSフォルダパスを指定）\n"
					);
		}
	}
}
