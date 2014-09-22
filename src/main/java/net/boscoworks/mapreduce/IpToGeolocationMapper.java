package net.boscoworks.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IpToGeolocationMapper extends Mapper <LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().replace("\n", "").replace("\r", "").split("\t");
		if (tokens.length == 15) {
			// loc_id, ip_start, ip_end, ip_start_str, latitude, longitude がnullの時は処理しない
			if (isEmptyString(tokens[0]) ||
					isEmptyString(tokens[1]) ||
					isEmptyString(tokens[2]) ||
                    isEmptyString(tokens[3]) ||
					isEmptyString(tokens[13]) ||
					isEmptyString(tokens[14])) {
				return;
			}

            String[] ip_values = tokens[3].split("\\.");
            // ip_start_str がドットつき十進数でない時は処理しない
            if (ip_values.length != 4) {
                return;
            }

            Long ip_start = Long.parseLong(tokens[1]);
            Long ip_end = Long.parseLong(tokens[2]);
            // ip_start が ip_end より大きい時は処理しない
            if (ip_start > ip_end) {
                return;
            }

            int count = 0;
            Boolean continue_flag = true;
            while (continue_flag == true) {
                Long ip_start_tmp = ip_start + (256 * 256 * count);
                Long ip_end_tmp = ip_start_tmp + (256 * 256);
                if (ip_end_tmp > ip_end) {
                    ip_end_tmp = ip_end;
                    continue_flag = false;
                }

                String key_str = ip_values[0] + "." + (Integer.parseInt(ip_values[1]) + count);
                StringBuffer sb = new StringBuffer();
                sb.append(tokens[0] + "\t");                                  // loc_id
                sb.append(ip_start_tmp + "\t");                               // lp_start
                sb.append(ip_end_tmp + "\t");                                 // lp_end
                sb.append(tokens[13] + "\t");                                 // latitude
                sb.append(tokens[14] + "\t");                                 // longitude
                sb.append(convertEmptyStringToNullString(tokens[5]) + "\t");  // country_code
                sb.append(convertEmptyStringToNullString(tokens[6]) + "\t");  // country_name
                sb.append(convertEmptyStringToNullString(tokens[7]) + "\t");  // region_code
                sb.append(convertEmptyStringToNullString(tokens[8]) + "\t");  // region_name
                sb.append(convertEmptyStringToNullString(tokens[9]) + "\t");  // city_name
                sb.append(convertEmptyStringToNullString(tokens[10]) + "\t"); // postal_code
                sb.append(convertEmptyStringToNullString(tokens[11]) + "\t"); // metro_code
                sb.append(convertEmptyStringToNullString(tokens[12]));         // area_code
                context.write(new Text(key_str), new Text(sb.toString()));
                count++;
            }
		} else {
			return;
		}
	}

	/**
	 * 空文字かnullであるかどうか判別する
	 * @param String str 入力文字列
	 * @return boolean 空文字かnullであればtrue、そうでなければfalse
	 */
	private static boolean isEmptyString(String str) {
		if ("".equals(str) || str == null) return true;
		else return false;
	}

	/**
	 * 空文字かnullであれば"null"に置換する
	 * @param String str 入力文字列
	 * @return String 文字列
	 */
	private static String convertEmptyStringToNullString(String str) {
		if (isEmptyString(str)) return "null";
		else return str;
	}
}
