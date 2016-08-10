package org.opengrid.data.impl;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.opengrid.constants.Exceptions;
import org.opengrid.data.GenericRetrievable;
import org.opengrid.data.meta.OpenGridColumn;
import org.opengrid.data.meta.OpenGridDataset;
import org.opengrid.data.meta.OpenGridMeta;
import org.opengrid.exception.ServiceException;
import org.opengrid.util.ExceptionUtil;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

import javax.annotation.Resource;
import org.opengrid.data.meta.DatasetOptions;
import org.opengrid.data.meta.QuickSearch;
import org.opengrid.data.meta.Rendition;
import org.opengrid.util.FileUtil;
import org.opengrid.util.PropertiesManager;
import org.opengrid.util.ServiceProperties;

public class PlenarioDataProvider implements GenericRetrievable {
    
        Logger log = Logger.getLogger(PlenarioDataProvider.class.getName());
        
        protected PropertiesManager properties = ServiceProperties.getProperties();
        

	@Override
	public String getData(String dataSetId, String metaCollectionName, String filter, int max, String sort, String options) throws ServiceException {		
			    	
            
                StringBuilder sURL = new StringBuilder();
                StringBuilder sb = new StringBuilder();
                
                sURL.append("http://plenar.io/v1/api/detail/?dataset_name=" + dataSetId + "&dup_ver=1&obs_date__ge=2000-1-1&data_type=geojson"); //just a string
                
                try
                {
                    
                    sURL.append(getGeoFilter(options));
                    
                    if (filter !=null && filter.length() > 0)
                    {
                        sURL.append(GetFilter(filter));
                    }
                    
                    //System.out.println("final string" + sURL.toString());
                    
                    OpenGridDataset desc = this.getDescriptorInternal(metaCollectionName, dataSetId, false);
                    
                    com.google.gson.JsonObject rootobj = getJsonObjectFromURL(sURL.toString());
                    JsonArray jsonArray = (JsonArray)rootobj.get("features");

                    sb.append("{ \"type\" : \"FeatureCollection\", \"features\" : [");
                    sb.append(getFeatures(jsonArray, desc));
                    sb.append("],");
                    sb.append(getMeta(metaCollectionName, dataSetId));
                    sb.append("}");
	        
                }
                catch(Exception ex)
                {
                    throw ExceptionUtil.getException(Exceptions.ERR_SERVICE, ex.getMessage());
                }
            
	    	//return geoJson object as part of our mock implementation
                
                return sb.toString();
	}

	
	private String getFeature(com.google.gson.JsonObject jsonElementMain, OpenGridDataset desc) {

		String s = "{\"type\": \"Feature\", \"properties\": ";
		com.google.gson.JsonObject jsonElement = (com.google.gson.JsonObject)jsonElementMain.get("properties");
                com.google.gson.JsonObject jsonElementGeo = (com.google.gson.JsonObject)jsonElementMain.get("geometry");
		Document doc = new Document();
		//iterate through available columns and build JSON		
		for (OpenGridColumn c: desc.getColumns()) {
			
			//support dotNotation on the id
			//if (c.getDataSource() == null) {
			//	c.setDataSource(c.getId());
			//}
			String colName = resolveName(c.getId());
			if ( jsonElement.has(colName) ) {
                            if(!colName.equals("location") && !jsonElement.get(colName).isJsonNull())
                            {
                                if(((JsonPrimitive)(jsonElement.get(colName))).isString())
                                {
                                    doc.put(c.getId(), jsonElement.get(colName).getAsString() );
                                }
                                else if(((JsonPrimitive)(jsonElement.get(colName))).isNumber())
                                {
                                    if(jsonElement.get(colName).getAsNumber().floatValue() == jsonElement.get(colName).getAsNumber().intValue())
                                        doc.put(c.getId(), jsonElement.get(colName).getAsNumber().intValue() ); 
                                    else
                                    {
                                        doc.put(c.getId(), jsonElement.get(colName).getAsNumber().floatValue() ); 
                                    }
                                }
                                else if(((JsonPrimitive)(jsonElement.get(colName))).isBoolean())
                                {
                                    doc.put(c.getId(), jsonElement.get(colName).getAsString() );
                                }
                                else
                                {
                                    
                                    doc.put(c.getId(),"");
                                }
                            }
                            else
                            {
                                if(c.getDataType().equals("string") || c.getDataType().equals("date"))
                                {
                                    doc.put(c.getId(),"");
                                }
                                else
                                    doc.put(c.getId(),0);
                            }
			}
		}
		
                
                /*if (!doc.containsKey("Longitude")) {
			//default longitude value
			doc.put("Longitude", jsonElementGeo.get("coordinates").getAsJsonArray().get(0).toString());
			//throw ExceptionUtil.getException(Exceptions.ERR_SERVICE, "Data is missing required '" +  desc.getOptions().getLong() + "' attribute for document " + doc.toJson());
		}*/
		
		String lng = jsonElementGeo.get("coordinates").getAsJsonArray().get(0).toString();
		
		/*if (!doc.containsKey("Latitude")) {
			//throw ExceptionUtil.getException(Exceptions.ERR_SERVICE, "Data is missing required '" +  desc.getOptions().getLat() + "' attribute for document " + doc.toJson());
			//default latitude value
			doc.put("Latitude", jsonElementGeo.get("coordinates").getAsJsonArray().get(1).toString());
		}*/
		
		String lat = jsonElementGeo.get("coordinates").getAsJsonArray().get(1).toString();
		s += doc.toJson();
		s +=", \"geometry\": {\"type\": \"Point\", \"coordinates\": [" + lng + "," + lat + "]}, \"autoPopup\": false }";
		return s;		
	}
	
	public String resolveName(String dataSource) {
		String[] s = dataSource.split("\\.");
		if (s.length > 0)
			return s[s.length - 1];
		else
			return dataSource;
	}

	//get Document that owns the field
	public Document resolveObject(Document doc2, String dataSource) {
		String[] s = dataSource.split("\\.");

		Document o = doc2;
		if (s.length > 1) {
			for (String c: s) {
				if (o.get(c) instanceof Document)				
					o = (Document) o.get(c);
				else
					break;
			}			
		}
		return o;
	}


	private String getMeta(String metaCollectionName, String dataSetId) throws JsonParseException, JsonMappingException, ServiceException, IOException {
		//return default descriptor, can be overridden by user preferences later
		//return "\"meta\": { \"view\": { \"id\": \"twitter\", \"displayName\": \"Twitter\", \"options\": { \"rendition\": { \"icon\":\"default\", \"color\": \"#001F7A\", \"fillColor\": \"#00FFFF\", \"opacity\":85, \"size\":6 } }, \"columns\": [ {\"id\":\"_id\", \"displayName\":\"ID\", \"dataType\":\"string\", \"filter\":false, \"popup\":false, \"list\":false}, {\"id\":\"date\", \"displayName\":\"Date\", \"dataType\":\"date\", \"filter\":true, \"popup\":true, \"list\":true, \"sortOrder\":1}, {\"id\":\"screenName\", \"displayName\":\"Screen Name\", \"dataType\":\"string\", \"filter\":true, \"popup\":true, \"list\":true, \"sortOrder\":2}, {\"id\":\"text\", \"displayName\":\"Text\", \"dataType\":\"string\", \"filter\":true, \"popup\":true, \"list\":true, \"sortOrder\":3}, {\"id\":\"city\", \"displayName\":\"City\", \"dataType\":\"string\", \"filter\":true, \"popup\":true, \"list\":true, \"sortOrder\":4}, {\"id\":\"bio\", \"displayName\":\"Bio\", \"dataType\":\"string\",\"sortOrder\":5}, {\"id\":\"hashtags\", \"displayName\":\"Hashtags\", \"dataType\":\"string\", \"sortOrder\":6}, {\"id\":\"lat\", \"displayName\":\"Latitude\", \"dataType\":\"float\", \"list\":true, \"sortOrder\":7}, {\"id\":\"long\", \"displayName\":\"Longitude\", \"dataType\":\"float\", \"list\":true, \"sortOrder\":8} ] } }";
		return "\"meta\": { \"view\": " + getDescriptor(metaCollectionName, dataSetId).toString() + " }";
	}

	
	
	@Override
	public OpenGridDataset getDescriptor(String metaCollectionName, String dataSetId) throws ServiceException, JsonParseException, JsonMappingException, IOException {
		return this.getDescriptorInternal(metaCollectionName, dataSetId, true);		
	}

	
	
	@Override
	public OpenGridDataset getDescriptorInternal(String metaCollectionName, String dataSetId, boolean removePrivates) throws ServiceException, JsonParseException, JsonMappingException, IOException {
		
            JsonObject datasets = getPlenarioDatasetJson();
            
            OpenGridDataset desc = getPlenarioDataset(datasets, dataSetId);

            if (removePrivates) {
                    //nullify some private info
                    desc.setDataSource(null);
            }

            if (desc == null)
                    throw new ServiceException("Cannot find dataset descriptor from meta store for dataset '" + dataSetId + "'.");
            return desc;    

	}


	@Override
	public List<String> getAllDatasetIds(String metaCollectionName)
			throws ServiceException, JsonParseException, JsonMappingException, IOException {
		List<String> a = new ArrayList<String>();
		
                com.google.gson.JsonObject datasets = getPlenarioDatasetJson();
                
                try{
                    OpenGridMeta meta = mapPlenarioToOpenGridMeta(datasets, true, true);
                //OpenGridMeta meta = (new ObjectMapper()).readValue(FileUtil.getJsonFileContents("json/datasets.json"), OpenGridMeta.class);
                
                 for (OpenGridDataset o: meta.getDatasets()) {
                        a.add(o.getId());
                    }	
                }
                catch(Exception e)
                {
                    String e_out = e.toString();
                    
                }
               		
                return a;
	}

        private com.google.gson.JsonObject getPlenarioDatasetJson()
        {
            String returnString = "";
            
            try
            {
                String sURL = "http://plenar.io/v1/api/datasets/"; //just a string

                return getJsonObjectFromURL(sURL);

            }
            catch(Exception e)
            {
                throw new ServiceException("Error accessing Plenar.io Datasets API.");
            }
            
        }
        
        private String GetFilter(String filter) throws ParseException, UnsupportedEncodingException{
            StringBuilder sb = new StringBuilder();
            
            LinkedHashMap<String, Object> q = new LinkedHashMap<String, Object>();	    		    	
            q =  (LinkedHashMap<String, Object>) JSON.parse(filter);
            for (Map.Entry<String, Object> entry : q.entrySet()) {
                
                BasicDBList q_list = new BasicDBList();	    		    	
                q_list =  (BasicDBList) JSON.parse(entry.getValue().toString());
                Iterator iter=q_list.iterator();
                
                while (iter.hasNext()) {

                    LinkedHashMap<String, Object> q_filter = new LinkedHashMap<String, Object>();	    		    	
                    q_filter =  (LinkedHashMap<String, Object>) JSON.parse(((LinkedHashMap<String, Object>)(iter.next())).toString());

                    for (Map.Entry<String, Object> q_filter_in : q_filter.entrySet()) {
                        String key = q_filter_in.getKey();
                        
                        if(q_filter_in.getValue().toString().startsWith("{"))
                        {
                            for (Map.Entry<String, Object> entry_fil : ((LinkedHashMap<String, Object>)(JSON.parse(q_filter_in.getValue().toString()))).entrySet()) 
                            {
                                String key_fil = entry_fil.getKey();

                                if(key_fil.equals("$gte")){   key_fil = "__ge";   }
                                else if(key_fil.equals("$gt")){   key_fil = "__gt";   }
                                else if(key_fil.equals("$lt")){   key_fil = "__lt";   }
                                else if(key_fil.equals("$lte")){   key_fil = "__le";   }
                                else if(key_fil.equals("$ne")){   key_fil = "__ne";   }
                                else if(key_fil.equals("$in")){   key_fil = "__in";   }
                                else if(key_fil.equals("$regex")){   key_fil = "__ilike";   }
                                sb.append("&"+key+key_fil+"=");

                                if(key.toLowerCase().contains("date"))
                                {
                                    DateFormat df = new SimpleDateFormat("yyyy-M-d");
                                    
                                    if (entry_fil.getValue().toString().contains("/"))
                                    {
                                        DateFormat df_in = new SimpleDateFormat("MM/dd/yyyy");
                                        sb.append(df.format(df_in.parse(entry_fil.getValue().toString())));
                                    }
                                    else
                                    {
                                        sb.append(df.format(new Date((Long)entry_fil.getValue())));
                                    }
                                }
                                else
                                {
                                    if(key_fil.equals("$regex"))
                                    {
                                        if(entry_fil.getValue().toString().contains("^"))
                                        {
                                            sb.append(entry_fil.getValue()).append("%");
                                        }
                                        else
                                        {
                                            sb.append("%").append(entry_fil.getValue()).append("%");
                                        }
                                    }
                                    else
                                    {
                                        sb.append(entry_fil.getValue());
                                    }
                                    
                                }
                            }
                        }
                        else
                        {
                            //This is a Begins With Query
                            if(q_filter_in.getValue().toString().contains("^"))
                            {
                                sb.append("&"+key+"__ilike=");
                                sb.append(q_filter_in.getValue().toString().replace("^", "") + "%");
                            }
                            else
                            {
                                sb.append("&"+key+"=");

                                if(key.toLowerCase().contains("date"))
                                {
                                    DateFormat df = new SimpleDateFormat("yyyy-M-d");
                                    sb.append(df.format(new Date((Long)q_filter_in.getValue())));
                                }
                                else
                                {
                                    sb.append(q_filter_in.getValue());
                                }
                            }
                        }
                        
                    }
                    // now work with key and value...
                }
            }
            
            
            return sb.toString().replace(" ", "%20");
        }
        
    private String getGeoFilter(String options) throws ParseException, UnsupportedEncodingException{
        String geoType = "Polygon";
        //String geoCoordinates = properties.getStringProperty("plenario.geo.default");    
         
        String geoCoordinates = properties.getStringProperty("plenario.geo.default");
        
        if (options !=null && options.length() > 0) {
            BasicDBObject o =  (BasicDBObject) JSON.parse(options);

            if (o.containsField("geoFilter")) {
                BasicDBObject qo =   (BasicDBObject) o.get("geoFilter");
                
                geoType = (String)qo.get("type");

                BasicDBList coordinatesList =   (BasicDBList) qo.get("coordinates");

                geoCoordinates = coordinatesList.toString().replace(" ", "%20");

            } 
        }

        return "&location_geom__within={\"type\":\"Feature\",\"properties\":{},\"geometry\":{\"type\":\"" + geoType + "\",\"coordinates\":"
                    + geoCoordinates
                    + "}}";
    }

    private String getFeatures(JsonArray jsonArray, OpenGridDataset desc) 
            throws ServiceException, JsonParseException, JsonMappingException, IOException{
        StringBuilder sb = new StringBuilder();

        Iterator itr = jsonArray.iterator();
        int i=0;

        while(itr.hasNext())
        {
            if (i > 0)
                sb.append(",");

            sb.append(getFeature((JsonObject)itr.next(), desc));
            i++;
        }
            
        return sb.toString();
       }
        
    private String getFeatures_New(String sURL, OpenGridDataset desc, int max) 
            throws ServiceException, JsonParseException, JsonMappingException, IOException{
        StringBuilder sb = new StringBuilder();
        int count = 0;
        
        while(count < max)
        {
            if(count>0)
            {
                sURL = sURL + "&offset=" + count;
            }
            com.google.gson.JsonObject rootobj = getJsonObjectFromURL(sURL);
            JsonArray jsonArray = (JsonArray)rootobj.get("objects");

            Iterator itr = jsonArray.iterator();
            int i=0;

            while(itr.hasNext())
            {
                if (i > 0 || (count < max && count > 0))
                    sb.append(",");

                sb.append(getFeature((JsonObject)itr.next(), desc));
                i++;
            }
            
            if(i<1000)
            {
                break;
            }

        }
        
        return sb.toString();
    }

    private OpenGridMeta mapPlenarioToOpenGridMeta(JsonObject rootobj, boolean includeColumns, boolean cityOnly) {
        OpenGridMeta meta = new OpenGridMeta();
        
        try
        {
            JsonArray objects = (JsonArray)rootobj.get("objects");
            List<OpenGridDataset> openGridDatasets = new ArrayList<OpenGridDataset>();
            
            Iterator itr = objects.iterator();
            
            String defaultAttribution = properties.getStringProperty("plenario.attribution.default");
            String defaultDataset = properties.getStringProperty("plenario.dataset.default");

            while(itr.hasNext())
            {
                OpenGridDataset dataset = getOpenGridDatasetFromPlenarioObject(defaultAttribution, defaultDataset, (JsonObject)itr.next(), includeColumns, cityOnly);
                if(dataset != null)
                {
                    openGridDatasets.add(dataset);
                }
                //if(openGridDatasets.size()> 5)
                //    break;    
            }
            meta.setDatasets(openGridDatasets);
         
        }
        catch(Exception e)
        {
            throw new ServiceException("Error processing Plenar.io Datasets API.");
        }
        
                
        
        return meta;

    }
    
    private OpenGridDataset getOpenGridDatasetFromPlenarioObject(String defaultAttribution, String defaultDataset, JsonObject jsonObject, boolean needColumns, boolean cityOnly) 
    throws ServiceException, JsonParseException, JsonMappingException, IOException
    {
        OpenGridDataset dataset = new OpenGridDataset();
        QuickSearch qs = new QuickSearch();
        
        if( cityOnly && !(jsonObject.get("attribution").toString().contains(defaultAttribution) 
                || jsonObject.get("dataset_name").toString().contains(defaultDataset)) || IgnoreDataset(jsonObject.get("dataset_name").toString().replace("\"", "")))
        {
            return null;
        }
        
        dataset.setId(jsonObject.get("dataset_name").toString().replace("\"", ""));
        dataset.setDisplayName(jsonObject.get("human_name").toString().replace("\"", ""));
        
        DatasetOptions options = new DatasetOptions();
        options.setLatLong("latitude,longitude");
        
        Rendition rend = new Rendition();
        rend.setColor("cyan");
        rend.setFillColor("#f03");
        rend.setIcon("default");
        rend.setOpacity(80);
        rend.setSize(10);        
        options.setRendition(rend);
        
        qs.setEnable(true);
        qs.setTriggerWord(dataset.getId());
        qs.setTriggerAlias(dataset.getId());
        dataset.setQuickSearch(qs);
        
        dataset.setOptions(options);
        
        if(needColumns)
        {
            String sURL = "http://plenar.io/v1/api/fields/" + dataset.getId();

            JsonObject rootobj = getJsonObjectFromURL(sURL);

            if(rootobj == null)
                return null;
            
            JsonObject meta = (JsonObject)rootobj.get("meta");
            
            String metaStatus = meta.get("status").toString().replace("\"", "");

            if(metaStatus.equals("error"))
                return null;
            
            List<OpenGridColumn> columns = getColumnsForDataset(rootobj);   
            dataset.setColumns(columns);
        }
        
        return dataset;
    }

    private List<OpenGridColumn> getColumnsForDataset(JsonObject rootobj) {
        List<OpenGridColumn> columns = new ArrayList<OpenGridColumn>();
            
        try
        {
            JsonArray objects = (JsonArray)rootobj.get("objects");
            List<OpenGridDataset> openGridDatasets = new ArrayList<OpenGridDataset>();
            
            Iterator itr = objects.iterator();
            int count = 1;
        
            while(itr.hasNext())
            {
                OpenGridColumn column = new OpenGridColumn();
                JsonObject object = (JsonObject)(itr.next());
                
                column.setDisplayName(object.get("field_name").toString().replace("\"", ""));
                column.setId(object.get("field_name").toString().replace("\"", ""));
                column.setDataType(getOpenDataGridTypeFromPlenario(object.get("field_type").toString().replace("\"", "")));
                
                column.setPopup(shouldColumnBeInPopup(object.get("field_name").toString().replace("\"", "")));
                column.setFilter(shouldColumnBeInPopup(object.get("field_name").toString().replace("\"", "")));
                column.setGroupBy(shouldColumnBeInGroupBy(object.get("field_name").toString().replace("\"", "")));
                column.setQuickSearch(shouldColumnBeInQuickSearch(object.get("field_name").toString().replace("\"", "")));
                
                if(!IgnoreColumn(object.get("field_name").toString().replace("\"", "")))
                {
                    column.setList(true);
                    column.setSortOrder(count);
                    count++;
                }
                columns.add(column);
            }

            return columns;

        }
        catch(Exception e)
        {
            throw new ServiceException("Error accessing Plenar.io Datasets API.");
        }
    }

    private JsonObject getJsonObjectFromURL(String sURL)throws ServiceException, JsonParseException, JsonMappingException, IOException 
    {
        // Connect to the URL using java's native library
            URL url = new URL(sURL);
            HttpURLConnection request = (HttpURLConnection) url.openConnection();
            request.connect();

            if(request.getResponseCode() != 200)
                return null;
            // Convert to a JSON object to print data
            com.google.gson.JsonParser jp = new com.google.gson.JsonParser(); //from gson
            com.google.gson.JsonElement root = jp.parse(new InputStreamReader((InputStream) request.getContent())); //Convert the input stream to a json element
            return root.getAsJsonObject(); //May be an array, may be an object. 
        
    }
    
    private OpenGridDataset getPlenarioDataset(JsonObject datasets, String datasetId) {
        OpenGridDataset dataset = new OpenGridDataset();
        
        try
        {
            JsonArray objects = (JsonArray)datasets.get("objects");
            
            Iterator itr = objects.iterator();
            
            String defaultAttribution = properties.getStringProperty("plenario.attribution.default");
            String defaultDataset = properties.getStringProperty("plenario.dataset.default");
        
            while(itr.hasNext())
            {
                JsonObject object = (JsonObject)itr.next();
                String datasetName = object.get("dataset_name").toString().replace("\"", "");
                
                if(datasetName.equals(datasetId))
                {
                    dataset = getOpenGridDatasetFromPlenarioObject(defaultAttribution, defaultDataset, object, true, false);
                    break;
                }
                
            }
            
            return dataset;

        }
        catch(Exception e)
        {
            throw new ServiceException("Error accessing Plenar.io Datasets API.");
        }
    }

    private String getOpenDataGridTypeFromPlenario(String plenarioType) {
        if(plenarioType.equals("BOOLEAN") || plenarioType.equals("VARCHAR"))
        {
            return "string";
        }
        else if(plenarioType.equals("INTEGER"))
        {
            return "number";
        }
        else if(plenarioType.equals("DOUBLE PRECISION"))
        {
            return "float";
        }
        else if(plenarioType.equals("DATE") ||plenarioType.equals("TIMESTAMP WITHOUT TIME ZONE"))
        {
            return "date";
        }
        
        return "string";
    }

    private boolean shouldColumnBeInPopup(String columnName) {
        if(IgnoreColumn(columnName) || columnName.equals("latitude") || columnName.equals("longitude") 
                || columnName.equals("x_coordinate") || columnName.equals("y_coordinate"))
        {
            return false;
        }
        
        return true;
    }
    
    private boolean shouldColumnBeInGroupBy(String columnName) {
        if(columnName.equals("zip") || columnName.equals("zip_code") 
                || columnName.equals("ward"))
        {
            return true;
        }
        
        return false;
    }
    
    private boolean shouldColumnBeInQuickSearch(String columnName) {
        if(columnName.equals("zip") || columnName.equals("zip_code"))
        {
            return true;
        }
        
        return false;
    }

    private boolean IgnoreDataset(String datasetName) {
        LinkedHashMap<String, Object> q = new LinkedHashMap<String, Object>();	    		    	
        q =  (LinkedHashMap<String, Object>) JSON.parse(FileUtil.getJsonFileContents("json/plenario_datasets_to_ignore.json"));
        
        for (Map.Entry<String, Object> entry : q.entrySet()) {
                
                BasicDBList q_list = new BasicDBList();	    		    	
                q_list =  (BasicDBList) JSON.parse(entry.getValue().toString());
                Iterator iter=q_list.iterator();
                
                while (iter.hasNext()) {
                    LinkedHashMap<String, String> q_filter = new LinkedHashMap<String, String>();	    		    	
                    q_filter =  (LinkedHashMap<String, String>) JSON.parse(((LinkedHashMap<String, String>)(iter.next())).toString());
                    String q_filter_key = q_filter.get("id");
                    if(q_filter_key.equals(datasetName))
                    {
                        return true;
                    }
                }
        }
        
        return false;
    }

    private boolean IgnoreColumn(String columnName) {
        if(columnName.endsWith("_id") || columnName.equals("start_date")
                || columnName.equals("end_date") || columnName.equals("current_flag") || columnName.equals("dup_ver"))
        {
            return true;
        }
        return false;
    }
    
}
