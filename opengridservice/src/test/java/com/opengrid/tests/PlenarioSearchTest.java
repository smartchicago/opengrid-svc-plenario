package com.opengrid.tests;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.opengrid.data.GenericRetrievable;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import org.opengrid.data.impl.OmniMongoDataProvider;
import org.opengrid.data.impl.PlenarioDataProvider;
import org.opengrid.data.meta.OpenGridDataset;
import org.opengrid.util.ServiceProperties;

//TODO: mock 'online' Mongo calls
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PlenarioSearchTest {
	
	@BeforeClass  
	public static void initTest() throws Exception {
	}
	
	
	@AfterClass  
	public static void cleanupTest() {
	}
	
        //@Test
	public void t1a_SearchDatasetID() {
		GenericRetrievable gr = new PlenarioDataProvider();
		try {
			String a = gr.getData("cdph_environmental_inspections", 
					null, 
					"{}", //filter
					6000,
					null,
                                        null);
                        
                        com.google.gson.JsonParser parser = new com.google.gson.JsonParser();
                        JsonElement object = parser.parse(a);
                        
			assertTrue("Result cannot be null", a !=null);
		} catch (Exception ex) {
			assertTrue("Unexpected exception message on bad dataset ID", ex.getMessage().indexOf("Cannot find dataset descriptor") > -1);
		}
	}
        
	@Test
	public void t1_GetAllDatasetIds() {
		
		GenericRetrievable gr = new PlenarioDataProvider();
		List<String> rs = null;
				
		try {
			rs = gr.getAllDatasetIds("twitter");
			assertTrue("Result cannot be null", rs !=null);
		} catch (Exception ex) {
			assertTrue("Unexpected exception message on bad meta collection", ex.getMessage().indexOf("Cannot find 'datasets' document") > -1);
		}
		
	}
	
	//@Test
	public void t2_SearchDatasetID() {
		GenericRetrievable gr = new PlenarioDataProvider();
		try {
			String a = gr.getData("311_service_requests_alley_lights_out", 
					null, 
					"{\"$and\":[{\"creation_date\":{\"$gte\":1468463580000}}]}", //filter
					6000,
					null,
                                        null);
                        
                        com.google.gson.JsonParser parser = new com.google.gson.JsonParser();
                        JsonElement object = parser.parse(a);
                        
			assertTrue("Result cannot be null", a !=null);
		} catch (Exception ex) {
			assertTrue("Unexpected exception message on bad dataset ID", ex.getMessage().indexOf("Cannot find dataset descriptor") > -1);
		}
	}
	
	@Test
	public void t3_GetDatasetDescriptor() {
		GenericRetrievable gr = new PlenarioDataProvider();
                try
                {
                OpenGridDataset dataset = gr.getDescriptorInternal(null, "311_service_requests_pot_holes_reported", false);		
		assertTrue("Result cannot be null", dataset !=null);
                }
                catch(Exception ex)
                {
                    assertTrue("Unexpected exception message on bad dataset ID", ex.getMessage().indexOf("Cannot find dataset descriptor") > -1);
                }
	}
	
	@Test
	public void t4_GetAllDatasets() {
		GenericRetrievable gr = new PlenarioDataProvider();
                try
                {
                    String descriptors = "";
		
		List<String> ds = gr.getAllDatasetIds("");
                
		for (String s: ds) {
			if (!descriptors.isEmpty())
                            descriptors += ", ";
			
                        descriptors += gr.getDescriptorInternal(null,s, false);
		
		}
                    
                    assertTrue("Result cannot be null", descriptors !=null);
                }
                catch(Exception ex)
                {
                    assertTrue("Unexpected exception message on bad dataset ID", ex.getMessage().indexOf("Cannot find dataset descriptor") > -1);
                }
	}
        
        //@Test
	public void t5_SearchDatasetID() {
		GenericRetrievable gr = new PlenarioDataProvider();
		try {
			String a = gr.getData("311_service_requests_pot_holes_reported", 
					null, 
					"{\"$and\":[{\"zip\":60601},{\"start_date\":{\"$gte\":1438405200000}}]}", //filter
					6000,
					null,
                                        null);
                        
                        com.google.gson.JsonParser parser = new com.google.gson.JsonParser();
                        JsonElement object = parser.parse(a);
                        
			assertTrue("Result cannot be null", a !=null);
		} catch (Exception ex) {
			assertTrue("Unexpected exception message on bad dataset ID", ex.getMessage().indexOf("Cannot find dataset descriptor") > -1);
		}
	}
        
        //@Test
	public void t6_SearchDatasetID() {
		GenericRetrievable gr = new PlenarioDataProvider();
		try {
			String a = gr.getData("311_service_requests_pot_holes_reported", 
					null, 
					"{\"$and\":[{\"start_date\":{\"$gte\":\"12/01/2015\",\"$lte\":\"12/03/2015\"}}]}", //filter
					6000,
					null,
                                        null);
                        
                        com.google.gson.JsonParser parser = new com.google.gson.JsonParser();
                        JsonElement object = parser.parse(a);
                        
			assertTrue("Result cannot be null", a !=null);
		} catch (Exception ex) {
			assertTrue("Unexpected exception message on bad dataset ID", ex.getMessage().indexOf("Cannot find dataset descriptor") > -1);
		}
	}
        
        //@Test
	public void t7_SearchDatasetID() {
		GenericRetrievable gr = new PlenarioDataProvider();
		try {
			String a = gr.getData("311_service_requests_pot_holes_reported", 
					null, 
					"{\"$and\":[{\"service_request_number\":{\"$regex\":\"^6\"}}]}", //filter
					6000,
					null,
                                        null);
                        
                        
                        
                        com.google.gson.JsonParser parser = new com.google.gson.JsonParser();
                        JsonElement object = parser.parse(a);
                        
			assertTrue("Result cannot be null", a !=null);
		} catch (Exception ex) {
			assertTrue("Unexpected exception message on bad dataset ID", ex.getMessage().indexOf("Cannot find dataset descriptor") > -1);
		}
	}
        //@Test
	public void t8_SearchDatasetID() {
		GenericRetrievable gr = new PlenarioDataProvider();
		try {
			String a = gr.getData("311_service_requests_garbage_carts", 
					null, 
					"{\"$and\":[{\"current_activity\":\"1\"},{\"police_district\":2},{\"$and\":[{\"current_activity\":\"1\"},{\"creation_date\":{\"$gt\":1474651260000}}]}]}", //filter
					6000,
					null,
                                        null);
                        
                        
                        
                        com.google.gson.JsonParser parser = new com.google.gson.JsonParser();
                        JsonElement object = parser.parse(a);
                        
			assertTrue("Result cannot be null", a !=null);
		} catch (Exception ex) {
			assertTrue("Unexpected exception message on bad dataset ID", ex.getMessage().indexOf("Cannot find dataset descriptor") > -1);
		}
        }
}