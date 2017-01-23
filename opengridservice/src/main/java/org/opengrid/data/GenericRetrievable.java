package org.opengrid.data;

import java.io.IOException;
import java.util.List;

import org.opengrid.data.meta.OpenGridDataset;
import org.opengrid.exception.ServiceException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.opengrid.data.meta.OpenGridMeta;


public interface GenericRetrievable {
	String getData(String dataSetId, OpenGridMeta metaCollection, String filter, int max, String sort, String options) throws ServiceException;	
	OpenGridDataset getDescriptor(OpenGridMeta metaCollection, String dataSetId) throws ServiceException, JsonParseException, JsonMappingException, IOException;	
	OpenGridDataset getDescriptorInternal(OpenGridMeta metaCollection, String dataSetId, boolean removePrivates) throws ServiceException, JsonParseException, JsonMappingException, IOException;
	List<String> getAllDatasetIds(String metaCollectionName) throws ServiceException, JsonParseException, JsonMappingException, IOException;
        OpenGridMeta getAllDatasts();
}