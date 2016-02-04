import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class GasMapper extends Mapper<Text, Text, Text, Text> {

	Text OutputKey = new Text();
	Text OutputValue = new Text();


	@Override
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {

		String data = value.toString();
		String[] values;
		String[] statePrice;
		String[] keys;
		String outputKey, outputValue;

		//Reset the Key and Value variable
		OutputKey.set("");
		OutputValue.set("");

		try
		{
			//Format -> Month_MinPrice; Month_MaxPrice
			values = data.split(";");
			//Format -> StateCode_Year
			keys = key.toString().split("_");

			//Assign Year as key
			outputKey = keys[1];

			//Value Format -> StateCode_MinPrice_MaxPrice
			outputValue = keys[0];

			for(int i=0; i<values.length; i++)
			{
				statePrice = values[i].split("_");
				outputValue += "_" + statePrice[1];
			}

			OutputKey.set(outputKey);
			OutputValue.set(outputValue);

			context.write(OutputKey, OutputValue);
		}

		catch(Exception ex)
		{
			System.out.println(ex.getMessage());
		}

	}

}

