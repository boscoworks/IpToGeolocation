package test.net.boscoworks.mapreduce;

import net.boscoworks.mapreduce.IpToGeolocationMapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

public class IpToGeolocationMapperTest {
	private Mapper<LongWritable, Text, Text, Text> mapper;
	private MapDriver<LongWritable, Text, Text, Text> driver;
	
	@Before
	public void setUp(){
		mapper = new IpToGeolocationMapper();
		driver = new MapDriver<LongWritable, Text, Text, Text>(mapper);
	}
	
	@Test
	public void test_ok_1record() throws IOException, InterruptedException {
		String input_value = "113100\t1728421888\t1728421888\t103.5.160.0\t103.5.160.0\tJP\tJapan\t40\tTokyo\tGinza\t\t\t\t35.6667\t139.7667";
		String output_key = "103.5";
		String output_value = "113100\t1728421888\t1728421888\t35.6667\t139.7667\tJP\tJapan\t40\tTokyo\tGinza\tnull\tnull\tnull";
		driver.withInput(new LongWritable(), new Text(input_value));
		driver.withOutput(new Text(output_key), new Text(output_value));
		driver.runTest();
	}

    @Test
    public void test_ok_2records() throws IOException, InterruptedException {
        String input_value = "113100\t1728421888\t1728487425\t103.5.160.0\t103.6.160.1\tJP\tJapan\t40\tTokyo\tGinza\t\t\t\t35.6667\t139.7667";
        String output_key1 = "103.5";
        String output_value1 = "113100\t1728421888\t1728487424\t35.6667\t139.7667\tJP\tJapan\t40\tTokyo\tGinza\tnull\tnull\tnull";
        String output_key2 = "103.6";
        String output_value2 = "113100\t1728487424\t1728487425\t35.6667\t139.7667\tJP\tJapan\t40\tTokyo\tGinza\tnull\tnull\tnull";
        driver.withInput(new LongWritable(), new Text(input_value));
        driver.withOutput(new Text(output_key1), new Text(output_value1));
        driver.withOutput(new Text(output_key2), new Text(output_value2));
        driver.runTest();
    }

    @Test
	public void test_loc_idがなければ出力しない() throws IOException, InterruptedException {
		String input_value = "\t1728421888\t1728421888\t103.5.160.0\t103.5.160.0\tJP\tJapan\t40\tTokyo\tGinza\t\t\t\t35.6667\t139.7667";
		driver.withInput(new LongWritable(), new Text(input_value));
		driver.runTest();
	}

	@Test
	public void test_ip_startがなければ出力しない() throws IOException, InterruptedException {
		String input_value = "113100\t\t1728421888\t103.5.160.0\t103.5.160.0\tJP\tJapan\t40\tTokyo\tGinza\t\t\t\t35.6667\t139.7667";
		driver.withInput(new LongWritable(), new Text(input_value));
		driver.runTest();
	}

	@Test
	public void test_ip_endがなければ出力しない() throws IOException, InterruptedException {
		String input_value = "113100\t1728421888\t\t103.5.160.0\t103.5.160.0\tJP\tJapan\t40\tTokyo\tGinza\t\t\t\t35.6667\t139.7667";
		driver.withInput(new LongWritable(), new Text(input_value));
		driver.runTest();
	}

	@Test
	public void test_latitudeがなければ出力しない() throws IOException, InterruptedException {
		String input_value = "113100\t1728421888\t1728421888\t103.5.160.0\t103.5.160.0\tJP\tJapan\t40\tTokyo\tGinza\t\t\t\t\t139.7667";
		driver.withInput(new LongWritable(), new Text(input_value));
		driver.runTest();
	}

	@Test
	public void test_longitudeがなければ出力しない() throws IOException, InterruptedException {
		String input_value = "113100\t1728421888\t1728421888\t103.5.160.0\t103.5.160.0\tJP\tJapan\t40\tTokyo\tGinza\t\t\t\t35.6667\t";
		driver.withInput(new LongWritable(), new Text(input_value));
		driver.runTest();
	}

    @Test
    public void test_ip_start_strがなければ出力しない() throws IOException, InterruptedException {
        String input_value = "113100\t1728421888\t1728487425\\t103.6.160.1\tJP\tJapan\t40\tTokyo\tGinza\t\t\t\t35.6667\t139.7667";
        driver.withInput(new LongWritable(), new Text(input_value));
        driver.runTest();
    }
}
