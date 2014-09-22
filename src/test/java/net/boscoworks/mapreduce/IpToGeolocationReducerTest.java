package test.net.boscoworks.mapreduce;

import java.util.Arrays;

import java.io.IOException;

import net.boscoworks.mapreduce.IpToGeolocationReducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class IpToGeolocationReducerTest {
	private Reducer<Text, Text, Text, Text> reducer;
	private ReduceDriver<Text, Text, Text, Text> driver;

	@Before
	public void setUp() {
		reducer = new IpToGeolocationReducer();
		driver = new ReduceDriver<Text, Text, Text, Text>(reducer);
	}

	@Test
	public void test_ok_1in_1out() throws IOException, InterruptedException {
		Text input_key = new Text("103.9");
		Text input_value = new Text("1\t1728685056\t1728685056\t35.6667\t139.7667\tJP\tJapan\t40\tTokyo\tGinza\tnull\tnull\tnull");
        Text output_key = new Text("1");
        Text output_value = new Text("103.9.164.0\t35.6667\t139.7667\tJP\tJapan\t40\tTokyo\tGinza\tnull\tnull\tnull");
		driver.withInput(input_key, Arrays.asList(input_value));
		driver.withOutput(output_key, output_value);
		driver.runTest();
	}

	@Test
	public void test_ok_1in_2out() throws IOException, InterruptedException {
        Text input_key = new Text("103.9");
        Text input_value = new Text("1\t1728685056\t1728685057\t35.6667\t139.7667\tJP\tJapan\t40\tTokyo\tGinza\tnull\tnull\tnull");
        Text output_key = new Text("1");
        Text output_value1 = new Text("103.9.164.0\t35.6667\t139.7667\tJP\tJapan\t40\tTokyo\tGinza\tnull\tnull\tnull");
		Text output_value2 = new Text("103.9.164.1\t35.6667\t139.7667\tJP\tJapan\t40\tTokyo\tGinza\tnull\tnull\tnull");
		driver.withInput(input_key, Arrays.asList(input_value));
		driver.withOutput(output_key, output_value1);
		driver.withOutput(output_key, output_value2);
		driver.runTest();
	}

    @Test
    public void test_ok_2in_2out() throws IOException, InterruptedException {
        Text input_key = new Text("103.9");
        Text input_value1 = new Text("1\t1728685056\t1728685056\t35.6667\t139.7667\tJP\tJapan\t40\tTokyo\tGinza\tnull\tnull\tnull");
        Text input_value2 = new Text("1\t1728685057\t1728685057\t35.6667\t139.7667\tJP\tJapan\t40\tTokyo\tGinza\tnull\tnull\tnull");
        Text output_key = new Text("1");
        Text output_value1 = new Text("103.9.164.0\t35.6667\t139.7667\tJP\tJapan\t40\tTokyo\tGinza\tnull\tnull\tnull");
        Text output_value2 = new Text("103.9.164.1\t35.6667\t139.7667\tJP\tJapan\t40\tTokyo\tGinza\tnull\tnull\tnull");
        driver.withInput(input_key, Arrays.asList(input_value1, input_value2));
        driver.withOutput(output_key, output_value1);
        driver.withOutput(output_key, output_value2);
        driver.runTest();
    }
}
