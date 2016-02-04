import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class StateMapper extends Mapper<LongWritable, Text, Text, Text> {

	Text OutputKey = new Text();
	Text OutputValue = new Text();


	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String data = value.toString();
		String[] values;

		//Reset the Key and Value variable
		OutputKey.set("");
		OutputValue.set("");
		
		try
		{
			values = data.split(",");
			context.getCounter(counters.TOTAL_RECORDS).increment(1);

			/* Expected Input Format (Comma separated fields - State(2 Char), YearMonth(6 digits), Price (Float)
			 * YearMonth and Price will be validated since State code has been used as input by the data sourcing program
			 * */

			int year=0;
			int month=0;
			
			//Mapper expects atleast 3 fields in the input text
			
			if(values.length >=3)
			{
				if(values[1].length() > 4)
				{
					year = Integer.parseInt(values[1].substring(0, 4));
					month = Integer.parseInt(values[1].substring(4));

					/*
					 * values[0] - State code
					 * values[1] - YYYYmm (Year & Month)
					 * value[2] - Gasoline Price
					 * */
					OutputKey.set(values[0] + "_" + year);						
				}
				else
				{
					//Invalid period format
					context.getCounter(counters.BAD_RECORDS).increment(1);
				}
				
				Float price = Float.parseFloat(values[2]);
				if ((month > 0 && month <= 12) && (year > 0))
				{
					OutputValue.set(month + "_" + price);
				}
				else
				{
					//Invalid month in the period parameter
					context.getCounter(counters.BAD_RECORDS).increment(1);
				}			

			}
			else
			{
				//Less than 3 parameters in input record
				context.getCounter(counters.BAD_RECORDS).increment(1);
			}
			
			if (OutputKey.toString().length() > 0 && OutputValue.toString().length() > 0)
				context.write(OutputKey, OutputValue);
		}
		
		catch(NumberFormatException nfe)
		{
			context.getCounter(counters.BAD_RECORDS).increment(1);
			System.out.println("Invalid data found on numerical field - " + nfe.getMessage());
		}

		catch(Exception ex)
		{
			//Unable to process the input record
			context.getCounter(counters.ERROR_RECORDS).increment(1);
			System.out.println(ex.getMessage());
		}

	}

}
