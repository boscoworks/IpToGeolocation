package net.boscoworks.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IpToGeolocationReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value: values) {
			String[] tokens = value.toString().split("\t");
            Long ip_start = Long.parseLong(tokens[1]);
            Long ip_end = Long.parseLong(tokens[2]);
            for (Long ip = ip_start; ip <= ip_end; ip++) {
                StringBuffer sb = new StringBuffer();
                sb.append(intToIpAddress(ip) + "\t"); // ip_address
                sb.append(tokens[3] + "\t");          // latitude
                sb.append(tokens[4] + "\t");          // longitude
                sb.append(tokens[5] + "\t");          // country_code
                sb.append(tokens[6] + "\t");          // country_name
                sb.append(tokens[7] + "\t");          // region_code
                sb.append(tokens[8] + "\t");          // region_name
                sb.append(tokens[9] + "\t");          // city_name
                sb.append(tokens[10] + "\t");         // postal_code
                sb.append(tokens[11] + "\t");         // metro_code
                sb.append(tokens[12]);                 // area_code
                context.write(new Text(tokens[0]), new Text(sb.toString()));
            }
		}
	}

    /**
     * int値IPアドレスをドットつき十進数IPアドレスに変換する
     * @param int value int値IPアドレス
     * @return String ドットつき十進数IPアドレス
     */
    private static String intToIpAddress(Long value) {
        Long ip1 = (value >> 24) & 0xff;
        Long ip2 = (value >> 16) & 0xff;
        Long ip3 = (value >> 8) & 0xff;
        Long ip4 = value & 0xff;
        return ip1 + "." + ip2 + "." + ip3 + "." + ip4;
    }
}
