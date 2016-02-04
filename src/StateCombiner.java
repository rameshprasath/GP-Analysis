import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class StateCombiner extends Reducer<Text, Text, Text, Text> {
	Text OutputValue = new Text();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		float minPrice = Float.MAX_VALUE;
		float maxPrice = Float.MIN_VALUE;
		int minPriceMonth=-1, maxPriceMonth=-1;
		String[] price;

		OutputValue.set("");

		try
		{
			for (Text value : values) {
				price = value.toString().split("_");

				if (Float.parseFloat(price[1]) > maxPrice)
				{
					maxPrice = Float.parseFloat(price[1]);
					maxPriceMonth = Integer.parseInt(price[0]);
				}
				else if (Float.parseFloat(price[1]) < minPrice)
					{
						minPrice = Float.parseFloat(price[1]);
						minPriceMonth = Integer.parseInt(price[0]);
					}
			}	
			
			OutputValue.set(minPriceMonth + "_" + minPrice + ";" + maxPriceMonth + "_" + maxPrice);
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
