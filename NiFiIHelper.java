import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;


public class NifiHelperClass {

	//gets the access token 
	public static String getNifiToken() throws IOException {
		OkHttpClient client = new OkHttpClient.Builder()
		        .connectTimeout(60, TimeUnit.SECONDS)
		        .writeTimeout(60, TimeUnit.SECONDS)
		        .readTimeout(60, TimeUnit.SECONDS)
		        .build();
	
		String token = null;
		Response response = null;

		try {
			MediaType mediaType = MediaType.parse("application/x-www-form-urlencoded");
			//every x days the password needs to be changed
			RequestBody body = RequestBody.create(mediaType, ReadPropertiesFile.nifiAccess);
			Request request = new Request.Builder()
			  .url("https://" + ReadPropertiesFile.nifiURL + "/nifi-api/access/token")
			  .post(body)
			  .addHeader("Content-Type", "application/x-www-form-urlencoded")
			  .addHeader("Accept", "*/*")
			  .addHeader("Cache-Control", "no-cache")
			  .addHeader("Host", ReadPropertiesFile.nifiURL)
			  .addHeader("accept-encoding", "deflate")
			  .addHeader("content-length", "47")
			  .addHeader("Connection", "keep-alive")
			  .addHeader("cache-control", "no-cache")
			  .build();
	
			response = client.newCall(request).execute();
			token = response.body().string();
			response.close();
		}
		catch(JSONException  | SocketTimeoutException se ){
	        System.out.println (se.toString());
	        response.close();
	    }
		response.close();
		return token;
	}
	
	//clears one queue
	public static void clearQueue(String token, String processID) throws IOException{
		OkHttpClient client = new OkHttpClient.Builder()
		        .connectTimeout(60, TimeUnit.SECONDS)
		        .writeTimeout(60, TimeUnit.SECONDS)
		        .readTimeout(60, TimeUnit.SECONDS)
		        .build();
		
		RequestBody body = RequestBody.create(null, new byte[0]);
		Request request = new Request.Builder()
				  .url("https://" + ReadPropertiesFile.nifiURL + "/nifi-api/flowfile-queues/" + processID + "/drop-requests")
				  .post(body)
				  .addHeader("Content-Type", "application/x-www-form-urlencoded")
				  .addHeader("Authorization", "Bearer "+ token)
				  .addHeader("Accept", "*/*")
				  .addHeader("Cache-Control", "no-cache")
				  .addHeader("Host", ReadPropertiesFile.nifiURL)
				  .addHeader("accept-encoding", "deflate")
				  .addHeader("content-length", "")
				  .addHeader("Connection", "keep-alive")
				  .addHeader("cache-control", "no-cache")
				  .build();
		Response response = client.newCall(request).execute();
		response.close();
	}
	
	//clear all queues
	public static boolean clearAllQueues(String token, String processGroupID) throws IOException {
		OkHttpClient client = new OkHttpClient.Builder()
		        .connectTimeout(60, TimeUnit.SECONDS)
		        .writeTimeout(60, TimeUnit.SECONDS)
		        .readTimeout(60, TimeUnit.SECONDS)
		        .build();

		Response response = null;
		
		try {
			Request request = new Request.Builder()
					  .url("https://" + ReadPropertiesFile.nifiURL + "/nifi-api/process-groups/" + processGroupID + "/connections")
					  .get()
					  .addHeader("Content-Type", "application/x-www-form-urlencoded")
					  .addHeader("Authorization", "Bearer " + token)
					  .addHeader("Accept", "*/*")
					  .addHeader("Cache-Control", "no-cache")
					  .addHeader("Host", ReadPropertiesFile.nifiURL)
					  .addHeader("accept-encoding", "deflate")
					  .addHeader("Connection", "keep-alive")
					  .addHeader("cache-control", "no-cache")
					  .build();

			response = client.newCall(request).execute();
					
			JSONObject JSONresponse =new JSONObject(response.body().string());
			JSONArray allConnectionsJSON = JSONresponse.getJSONArray("connections");
					
					
					

			for(int i = 0; i < allConnectionsJSON.length(); i++) {
				JSONObject connection = allConnectionsJSON.getJSONObject(i);
				String connectionID = connection.getString("id");
						
				clearQueue(token,connectionID);
						
			}
			response.close();
			return true;
			
		}
		catch(JSONException  | SocketTimeoutException se ){
	        System.out.println (se.toString());
	        response.close();
	        return false;
	    }

		
		
	}
	
	//turns on the processor
	public static boolean processorOn(String token, String processorID) throws IOException{
		String clientID = null;
		Response response = null;

		try {
			OkHttpClient client = new OkHttpClient.Builder()
			        .connectTimeout(60, TimeUnit.SECONDS)
			        .writeTimeout(60, TimeUnit.SECONDS)
			        .readTimeout(60, TimeUnit.SECONDS)
			        .build();
			
			Request request = new Request.Builder()
					  .url("https://" + ReadPropertiesFile.nifiURL + "/nifi-api/processors/" + processorID)
					  .get()
					  .addHeader("Content-Type", "application/json")
					  .addHeader("Authorization", "Bearer " + token)
					  .addHeader("Accept", "*/*")
					  .addHeader("Cache-Control", "no-cache")
					  .addHeader("Host", "azcedlnificd02.mfcgd.com:9091")
					  .addHeader("accept-encoding", "deflate")
					  .addHeader("Connection", "keep-alive")
					  .addHeader("cache-control", "no-cache")
					  .build();

			response = client.newCall(request).execute();

			JSONObject JSONresponse =new JSONObject(response.body().string());

			clientID = JSONresponse.getJSONObject("revision").getString("clientId");
			
			
			//Turn on Processor
			MediaType mediaType = MediaType.parse("application/json");
			RequestBody body = RequestBody.create(mediaType, "{\"component\": {\"state\": \"RUNNING\",\"id\":\""+ processorID +"\"},\"revision\": {\"version\": 0, \"clientId\":\""+ clientID +"\"}}");
			request = new Request.Builder()
			  .url("https://" + ReadPropertiesFile.nifiURL + "/nifi-api/processors/" + processorID)
			  .put(body)
			  .addHeader("Content-Type", "application/json")
			  .addHeader("Authorization", "Bearer " + token)
			  .addHeader("Accept", "*/*")
			  .addHeader("Cache-Control", "no-cache")
			  .addHeader("Host", "azcedlnificd02.mfcgd.com:9091")
			  .addHeader("accept-encoding", "deflate")
			  .addHeader("content-length", "159")
			  .addHeader("Connection", "keep-alive")
			  .addHeader("cache-control", "no-cache")
			  .build();

			response = client.newCall(request).execute();
			response.close();
			return true;
		}	
		catch(JSONException  | SocketTimeoutException se ){
	        System.out.println (se.toString());
	        response.close();
	        return false;
	    }
	}
	
	//turn off a processor
	public static boolean processorOff(String token, String processorID) throws IOException{
		String clientID = null;
		Response response = null;

		try {
			OkHttpClient client = new OkHttpClient.Builder()
			        .connectTimeout(60, TimeUnit.SECONDS)
			        .writeTimeout(60, TimeUnit.SECONDS)
			        .readTimeout(60, TimeUnit.SECONDS)
			        .build();
			
			Request request = new Request.Builder()
					  .url("https://" + ReadPropertiesFile.nifiURL + "/nifi-api/processors/" + processorID)
					  .get()
					  .addHeader("Content-Type", "application/json")
					  .addHeader("Authorization", "Bearer " + token)
					  .addHeader("Accept", "*/*")
					  .addHeader("Cache-Control", "no-cache")
					  .addHeader("Host", "azcedlnificd02.mfcgd.com:9091")
					  .addHeader("accept-encoding", "deflate")
					  .addHeader("Connection", "keep-alive")
					  .addHeader("cache-control", "no-cache")
					  .build();

			response = client.newCall(request).execute();

			JSONObject JSONresponse =new JSONObject(response.body().string());

			clientID = JSONresponse.getJSONObject("revision").getString("clientId");
				
			
			//Turn on Processor
			MediaType mediaType = MediaType.parse("application/json");
			RequestBody body = RequestBody.create(mediaType, "{\"component\": {\"state\": \"STOPPED\",\"id\":\""+ processorID +"\"},\"revision\": {\"version\": 0, \"clientId\":\""+ clientID +"\"}}");
			request = new Request.Builder()
			  .url("https://" + ReadPropertiesFile.nifiURL + "/nifi-api/processors/" + processorID)
			  .put(body)
			  .addHeader("Content-Type", "application/json")
			  .addHeader("Authorization", "Bearer " + token)
			  .addHeader("Accept", "*/*")
			  .addHeader("Cache-Control", "no-cache")
			  .addHeader("Host", "azcedlnificd02.mfcgd.com:9091")
			  .addHeader("accept-encoding", "deflate")
			  .addHeader("content-length", "159")
			  .addHeader("Connection", "keep-alive")
			  .addHeader("cache-control", "no-cache")
			  .build();

			response = client.newCall(request).execute();
			response.close();
			return true;
		}	
		catch(JSONException  | SocketTimeoutException se ){
	        System.out.println (se.toString());
	        response.close();
	        return false;
	    }
	}
	
	//turns on a process group
	public static boolean processGroupOn(String token, String processGroupID) throws IOException{
		
		try {
			OkHttpClient client = new OkHttpClient.Builder()
			        .connectTimeout(60, TimeUnit.SECONDS)
			        .writeTimeout(60, TimeUnit.SECONDS)
			        .readTimeout(60, TimeUnit.SECONDS)
			        .build();
	
			MediaType mediaType = MediaType.parse("application/json");
			RequestBody body = RequestBody.create(mediaType, "{\"id\":\""+processGroupID+"\",\"state\":\"RUNNING\"}");
			Request request = new Request.Builder()
			  .url("https://" + ReadPropertiesFile.nifiURL + "/nifi-api/flow/process-groups/" + processGroupID)
			  .put(body)
			  .addHeader("Content-Type", "application/json")
			  .addHeader("Authorization", "Bearer "+ token)
			  .addHeader("Accept", "*/*")
			  .addHeader("Cache-Control", "no-cache")
			  .addHeader("Host", ReadPropertiesFile.nifiURL)
			  .addHeader("accept-encoding", "deflate")
			  .addHeader("content-length", "63")
			  .addHeader("Connection", "keep-alive")
			  .addHeader("cache-control", "no-cache")
			  .build();
	
			Response response = client.newCall(request).execute();
			response.close();
			return true;
		}	
		catch(JSONException  | SocketTimeoutException se ){
	        System.out.println (se.toString());
	        processGroupOff(token,processGroupID);
	        return false;
	    }
	}
	
	//turns off a process group
	public static boolean processGroupOff(String token, String processGroupID) throws IOException{
		
		try {
			OkHttpClient client = new OkHttpClient.Builder()
			        .connectTimeout(60, TimeUnit.SECONDS)
			        .writeTimeout(60, TimeUnit.SECONDS)
			        .readTimeout(60, TimeUnit.SECONDS)
			        .build();
	
			MediaType mediaType = MediaType.parse("application/json");
			RequestBody body = RequestBody.create(mediaType, "{\"id\":\""+processGroupID+"\",\"state\":\"STOPPED\"}");
			Request request = new Request.Builder()
			  .url("https://" + ReadPropertiesFile.nifiURL + "/nifi-api/flow/process-groups/" + processGroupID)
			  .put(body)
			  .addHeader("Content-Type", "application/json")
			  .addHeader("Authorization", "Bearer "+ token)
			  .addHeader("Accept", "*/*")
			  .addHeader("Cache-Control", "no-cache")
			  .addHeader("Host", ReadPropertiesFile.nifiURL)
			  .addHeader("accept-encoding", "deflate")
			  .addHeader("content-length", "63")
			  .addHeader("Connection", "keep-alive")
			  .addHeader("cache-control", "no-cache")
			  .build();
	
			Response response = client.newCall(request).execute();
			response.close();
			return true;
		}	
		catch(JSONException  | SocketTimeoutException se ){
	        System.out.println (se.toString());
	        return false;
	    }
	}
	
	//check if db data import is done by adding the success and failur Q
	public static boolean checkImportDone(String token, String successQ, String failQ, String processGroupId) throws IOException, InterruptedException{
		OkHttpClient client = new OkHttpClient.Builder()
		        .connectTimeout(60, TimeUnit.SECONDS)
		        .writeTimeout(60, TimeUnit.SECONDS)
		        .readTimeout(60, TimeUnit.SECONDS)
		        .build();
		
		boolean success = false;
		int counter = 0;
		Response response = null;
		
		while(!success) {
			try {
				//check the queues every 3 seconds
				Thread.sleep(3000); 
				RequestBody body =RequestBody.create(null, new byte[0]);
				Request request = new Request.Builder()
						  .url("https://" + ReadPropertiesFile.nifiURL + "/nifi-api/flowfile-queues/"+successQ+"/listing-requests")
						  .post(body)
						  .addHeader("Content-Type", "application/x-www-form-urlencoded")
						  .addHeader("Authorization", "Bearer " + token)
						  .addHeader("Accept", "*/*")
						  .addHeader("Cache-Control", "no-cache")
						  .addHeader("Host", ReadPropertiesFile.nifiURL)
						  .addHeader("accept-encoding", "deflate")
						  .addHeader("content-length", "")
						  .addHeader("Connection", "keep-alive")
						  .addHeader("cache-control", "no-cache")
						  .build();

				response = client.newCall(request).execute();

				JSONObject JSONresponse =new JSONObject(response.body().string());

				int successQueuCount = JSONresponse.getJSONObject("listingRequest").getJSONObject("queueSize").getInt("objectCount");
				
				request = new Request.Builder()
						  .url("https://" + ReadPropertiesFile.nifiURL + "/nifi-api/flowfile-queues/"+failQ+"/listing-requests")
						  .post(body)
						  .addHeader("Content-Type", "application/x-www-form-urlencoded")
						  .addHeader("Authorization", "Bearer " + token)
						  .addHeader("Accept", "*/*")
						  .addHeader("Cache-Control", "no-cache")
						  .addHeader("Host", ReadPropertiesFile.nifiURL)
						  .addHeader("accept-encoding", "deflate")
						  .addHeader("content-length", "")
						  .addHeader("Connection", "keep-alive")
						  .addHeader("cache-control", "no-cache")
						  .build();

				response = client.newCall(request).execute();
				JSONObject JSONresponse2 =new JSONObject(response.body().string());
				int failQueuCount = JSONresponse2.getJSONObject("listingRequest").getJSONObject("queueSize").getInt("objectCount");
				counter++;
				response.close();

				if (successQueuCount>=1) {
					return true;
				}
				else if(failQueuCount>=1) {
					return false;
					
				}
				
				if(counter == 1200) {
					return false;
				}
			}
			catch(JSONException  | SocketTimeoutException se ){
		        System.out.println (se.toString());
		        response.close();
		        processGroupOff(token,processGroupId);
		        return false;
		    }


		}
		return false;
	}
	
	//check if db data import is done by adding the success and failur Q
	public static boolean checkSFImportDone(String token, String successQ, String failQ, String sourceQ, String processGroupId) throws IOException, InterruptedException{
		OkHttpClient client = new OkHttpClient.Builder()
		        .connectTimeout(60, TimeUnit.SECONDS)
		        .writeTimeout(60, TimeUnit.SECONDS)
		        .readTimeout(60, TimeUnit.SECONDS)
		        .build();
		int totalRecord = 100;
		boolean success = false;
		int counter = 0;
		Response response = null;

		
		while(!success) {
			try {
				//check the queues every 3 seconds
				Thread.sleep(9000); 

				//this gets the responseQueueURL so we can take a look at whats inside the queue
				RequestBody body =RequestBody.create(null, new byte[0]);
				Request request = new Request.Builder()
				  .url("https://" + ReadPropertiesFile.nifiURL + "/nifi-api/flowfile-queues/"+sourceQ+"/listing-requests")
				  .post(body)
				  .addHeader("Content-Type", "application/x-www-form-urlencoded")
				  .addHeader("Authorization", "Bearer " + token)
				  .addHeader("Accept", "*/*")
				  .addHeader("Cache-Control", "no-cache")
				  .addHeader("Host", ReadPropertiesFile.nifiURL)
				  .addHeader("accept-encoding", "deflate")
				  .addHeader("content-length", "")
				  .addHeader("Connection", "keep-alive")
				  .addHeader("cache-control", "no-cache")
				  .build();

				response = client.newCall(request).execute();
				
				
				
				//this gets the data url so we can access the data thats waiting inside of this queue 
				JSONObject JSONresponse =new JSONObject(response.body().string());
				String responseQueueURL = JSONresponse.getJSONObject("listingRequest").getString("uri");
				
				
				request = new Request.Builder()
						  .url(responseQueueURL)
						  .get()
						  .addHeader("Content-Type", "application/x-www-form-urlencoded")
						  .addHeader("Authorization", "Bearer " + token)
						  .addHeader("Accept", "*/*")
						  .addHeader("Cache-Control", "no-cache")
						  .addHeader("Host", ReadPropertiesFile.nifiURL)
						  .addHeader("accept-encoding", "deflate")
						  .addHeader("Connection", "keep-alive")
						  .addHeader("cache-control", "no-cache")
						  .build();

				response = client.newCall(request).execute();	
				JSONresponse =new JSONObject(response.body().string());
			
				
				JSONArray flowFileSummaries = JSONresponse.getJSONObject("listingRequest").getJSONArray("flowFileSummaries");
				

				if(flowFileSummaries.length()==0) {
					System.out.println("going back to the top again");
					response.close();
					continue;
				}				

				
				String responseDataURL = flowFileSummaries.getJSONObject(0).getString("uri");
				String clusterNodeId = flowFileSummaries.getJSONObject(0).getString("clusterNodeId");
				
				//extracts the data that we want form the flow-file which is the total record size 
				request = new Request.Builder()
						  .url(responseDataURL+"/content?clusterNodeId="+clusterNodeId)
						  .get()
						  .addHeader("Authorization", "Bearer " + token)
						  .addHeader("Accept", "*/*")
						  .addHeader("Cache-Control", "no-cache")
						  .addHeader("Host", ReadPropertiesFile.nifiURL)
						  .addHeader("accept-encoding", "deflate")
						  .addHeader("Connection", "keep-alive")
						  .addHeader("cache-control", "no-cache")
						  .build();

				response = client.newCall(request).execute();
				JSONresponse =new JSONObject(response.body().string());
				totalRecord = JSONresponse.getInt("totalSize");
				
				//cout the total amount of records in fail and success to see if the upload has been completed
				request = new Request.Builder()
						  .url("https://" + ReadPropertiesFile.nifiURL + "/nifi-api/flowfile-queues/"+successQ+"/listing-requests")
						  .post(body)
						  .addHeader("Content-Type", "application/x-www-form-urlencoded")
						  .addHeader("Authorization", "Bearer " + token)
						  .addHeader("Accept", "*/*")
						  .addHeader("Cache-Control", "no-cache")
						  .addHeader("Host", ReadPropertiesFile.nifiURL)
						  .addHeader("accept-encoding", "deflate")
						  .addHeader("content-length", "")
						  .addHeader("Connection", "keep-alive")
						  .addHeader("cache-control", "no-cache")
						  .build();

				response = client.newCall(request).execute();
				JSONresponse =new JSONObject(response.body().string());
				int successQueuCount = JSONresponse.getJSONObject("listingRequest").getJSONObject("queueSize").getInt("objectCount");
				
				request = new Request.Builder()
						  .url("https://" + ReadPropertiesFile.nifiURL + "/nifi-api/flowfile-queues/"+failQ+"/listing-requests")
						  .post(body)
						  .addHeader("Content-Type", "application/x-www-form-urlencoded")
						  .addHeader("Authorization", "Bearer " + token)
						  .addHeader("Accept", "*/*")
						  .addHeader("Cache-Control", "no-cache")
						  .addHeader("Host", ReadPropertiesFile.nifiURL)
						  .addHeader("accept-encoding", "deflate")
						  .addHeader("content-length", "")
						  .addHeader("Connection", "keep-alive")
						  .addHeader("cache-control", "no-cache")
						  .build();

				response = client.newCall(request).execute();
				JSONObject JSONresponse2 =new JSONObject(response.body().string());
				int failQueuCount = JSONresponse2.getJSONObject("listingRequest").getJSONObject("queueSize").getInt("objectCount");				
				response.close();
				
				counter++;
				if(successQueuCount + failQueuCount >= totalRecord) {
					success = true;
				}
				if(counter==1200) {
					return false;
				}
				System.out.println(successQueuCount + failQueuCount);
				System.out.println(totalRecord);
				System.out.println(counter);
			}
			catch(JSONException  | SocketTimeoutException se ){
		        System.out.println (se.toString());
		        response.close();
		        processGroupOff(token,processGroupId);
		        return false;
		    }


		}
		response.close();
		return success;
	}
}





