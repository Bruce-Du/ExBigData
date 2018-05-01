package brucedu.bigdata.hive_udf;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 《Hive编程指南》 13.6
 * 通过日期计算其星座
 */

@Description(name = "zodiac",
	value = "_FUNC_(date) - from the input date string " +
		"or separate month and day arguments, returns the sign of the Zodiac.",
		extended = "Example:\n"
			+ " > SELECT _FUNC_(date_string) FROM src;\n"
			+ " > SELECT _FUNC_(month, day) FROM src;")

//TODO:1.星座名用枚举表示 2.evaluate方法中的if用switch或函数式编程思想（带函数接口的map）
public class UDFZodiacSign extends UDF {

	private SimpleDateFormat df;

	public UDFZodiacSign() {
		df = new SimpleDateFormat("MM-dd-yyyy");
	}

	public String evaluate(Date bday){
		return this.evaluate(bday.getMonth(), bday.getDay());
	}

	public String evaluate(String bday){
		Date date = null;
		try{
			date = df.parse(bday);
		}catch(Exception ex){
			return null;
		}
		return this.evaluate(date.getMonth() + 1, date.getDay());
	}
	private String evaluate(int month, int day) {
		if(month == 1){
			if(day < 20){
				return "Capricorn";
			} else {
				return "Aquarius";
			}
		}

		if(month == 2){
			if(day < 19){
				return "Aquarius";
			} else {
				return "Pisces";
			}
		}

		/* ...other months here*/
		return null;
	}
}
