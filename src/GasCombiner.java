import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class GasCombiner extends Reducer<Text, Text, Text, Text> {

	Text OutputValue = new Text();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		float minPrice = Float.MAX_VALUE;
		float maxPrice = Float.MIN_VALUE;
		String minPriceState="", maxPriceState=""; 
		String[] info;

		OutputValue.set("");

		try
		{
			//Value format -> StateCode_MinPrice_MaxPrice
			for (Text value : values) {
				info = value.toString().split("_");

				if (Float.parseFloat(info[2]) > maxPrice)
				{
					maxPrice = Float.parseFloat(info[2]);
					maxPriceState = info[0];
				}
				if (Float.parseFloat(info[1]) < minPrice)
				{
					minPrice = Float.parseFloat(info[1]);
					minPriceState = info[0];
				}
			}	

//			OutputValue.set("Minimum gas price is " + minPrice + " on the state " 
//					+  minPriceState + ", and maximum gas price is " + maxPrice
//					+ " on the state " + maxPriceState);
			OutputValue.set(minPriceState + "_" + minPrice + "_" + maxPrice + ";" + maxPriceState);

			context.write(key, OutputValue);	
		}
		catch(NumberFormatException NFex)
		{
			System.out.println(NFex.getMessage());
		}
		catch(Exception ex)
		{
			System.out.println(ex.getMessage());
		}
	}

}