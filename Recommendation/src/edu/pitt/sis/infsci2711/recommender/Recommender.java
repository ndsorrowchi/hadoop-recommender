package edu.pitt.sis.infsci2711.recommender;

import java.io.IOException;
import java.util.Arrays;

import java.util.ArrayList;
import java.util.Collections;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Recommender extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
	   System.out.println(args.length);
	   if(args.length<3)// input args: <input> <temp> <output>
	   {System.exit(-1);}
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new Recommender(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      
      Job job1 = new Job(getConf(), "Recommender q1");
      job1.setJarByClass(Recommender.class);
      job1.setOutputKeyClass(Text.class);
      job1.setOutputValueClass(IntWritable.class);
      job1.setMapperClass(Map.class);
      job1.setReducerClass(Reduce.class);
      job1.setInputFormatClass(TextInputFormat.class);
      job1.setOutputFormatClass(TextOutputFormat.class);
      FileInputFormat.addInputPath(job1, new Path(args[0]));
      FileOutputFormat.setOutputPath(job1, new Path(args[1]));
      job1.waitForCompletion(true);
      
      Job job2 = new Job(getConf(), "Recommender q2");
      job2.setJarByClass(Recommender.class);
      job2.setOutputKeyClass(IntWritable.class);
      job2.setOutputValueClass(Text.class);
      job2.setMapperClass(Map2.class);
      job2.setReducerClass(Reduce2.class);
      job2.setInputFormatClass(TextInputFormat.class);
      job2.setOutputFormatClass(TextOutputFormat.class);
      FileInputFormat.addInputPath(job2, new Path(args[1]));
      FileOutputFormat.setOutputPath(job2, new Path(args[2]));
      job2.waitForCompletion(true);
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	  private final static IntWritable ZERO = new IntWritable(0);
	  private final static IntWritable ONE = new IntWritable(1);
      private Text word = new Text();
      
      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  
    	  StringTokenizer tokenizer = new StringTokenizer(value.toString(), " \t,");   	  
    	  if(!tokenizer.hasMoreTokens())//avoid getting errors in a line with nothing
    	  {
    		  return;
    	  }
    	  ArrayList<Integer> friendsList = new ArrayList<Integer>();
    	  int userid = Integer.parseInt(tokenizer.nextToken());//user should be the first one, before tab;
    	  while (tokenizer.hasMoreTokens())
    	  {
	    	  int friend = Integer.parseInt(tokenizer.nextToken());
	    	  friendsList.add(friend);
    	  }
    	  Collections.sort(friendsList);
    	  for (int i = 0; i < friendsList.size(); i++)
    	  {
    		  //get next friend
	    	  int f1 = friendsList.get(i);
	 
	    	  String s1;
	    	  if (userid <= f1)
	    	  {
	    		  s1 = Integer.toString(userid) + " " + Integer.toString(f1);
	    	  }
	    	  else
	    	  {
	    		  s1 = Integer.toString(f1) + " " + Integer.toString(userid);
	    	  }
	    	  //generate list of user and friend pairs as key; zero is value; because they are alreay friends.
	    	  context.write(new Text(s1), ZERO);
	    	  for (int j = i+1; j < friendsList.size(); j++)
	    	  {
	    		  //the next friend after f1
		    	  int f2 = friendsList.get(j);
		    	  //these people are mutual friends because they are friends of userid
		    	  String s2 = Integer.toString(f1) + " " + Integer.toString(f2);
		    	  //generate neutral friends calculated from userid
		    	  context.write(new Text(s2), ONE);
	    	  }
    	  }
      }
   }

   public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text>
   {
	   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	   {
		   StringTokenizer tokenizer = new StringTokenizer(value.toString());
		   // exit if we hit a blank line
		   if (!tokenizer.hasMoreTokens())
		   {
			   return;
		   }
		   int user1 = Integer.parseInt(tokenizer.nextToken());
		   int user2 = Integer.parseInt(tokenizer.nextToken());
		   int nMutual = Integer.parseInt(tokenizer.nextToken());
		   // string 1 means that user1 has n friends in common with user2
		   String s1 = user2 + " " + nMutual;
		   context.write(new IntWritable(user1), new Text(s1));
		   // string 2 means that user2 has n friends in common with user1
		   String s2 = user1 + " " + nMutual;
		   context.write(new IntWritable(user2), new Text(s2));
	   }
   }
   
   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
   {
	   public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	   {
		   int nMutual = 0;
		   for (IntWritable val : values)
		   {
			   // if we see a value of 0 then the two users are direct friends
			   // no need for recommendation, so no output
			   if (val.get() == 0)
			   { return; }
			   nMutual += val.get();
		   }
		   //after for loop, the result is that they are really mutual friends
		   //so record them, and the value is how many common friends they have
		   context.write(key, new IntWritable(nMutual));
	   }
   }
   
   public static class Reduce2 extends Reducer<IntWritable, Text, IntWritable, Text>
   {
	   public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	   {
		   int user = key.get();
		   TreeMap<Integer, ArrayList<Integer>> sortedMap = new TreeMap<Integer, ArrayList<Integer>>();
		   for (Text val : values)
		   {
			   StringTokenizer tokenizer = new StringTokenizer(val.toString());
			   int potentialFriend = Integer.parseInt(tokenizer.nextToken());
			   int mutualFriends = Integer.parseInt(tokenizer.nextToken());
			   if (sortedMap.get(mutualFriends) == null)
			   {
				   // there are no users yet with this number of mutual friends
				   ArrayList<Integer> arrayList = new ArrayList<Integer>();
				   arrayList.add(potentialFriend);
				   sortedMap.put(mutualFriends, arrayList);
			   }
			   else
			   {
				   // there are already users with this number of mutual friends
				   sortedMap.get(mutualFriends).add(potentialFriend);
			   }
		   }
		   // now select the top 10 users to recommend as potential friends
		   ArrayList<Integer> recommendations = new ArrayList<Integer>();
		   boolean exitLoops = false;
		   for (int mutualFriends:sortedMap.descendingKeySet())
		   {
			   ArrayList<Integer> potentialFriends = sortedMap.get(mutualFriends);
			   Collections.sort(potentialFriends);
			   for (int potentialFriend:potentialFriends)
			   {
				   recommendations.add(potentialFriend);
				   if (recommendations.size() == 10)
				   {
					   exitLoops = true;
					   break;
				   }
			   }
			   if (exitLoops)
			   break;
		   }
		   if (recommendations.size() == 0)
		   { return; }
		   String s = null;
		   for (int recommendation:recommendations)
		   {
			   if (s == null)
			   {s = Integer.toString(recommendation);}
			   else
			   {s += "," + recommendation;}
		   }
		   context.write(new IntWritable(user), new Text(s));
		   }
   }
   
}