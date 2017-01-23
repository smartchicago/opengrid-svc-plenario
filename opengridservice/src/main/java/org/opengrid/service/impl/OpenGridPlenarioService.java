package org.opengrid.service.impl;

import java.io.IOException;
import java.util.List;

import io.jsonwebtoken.Claims;

import org.apache.cxf.jaxrs.ext.MessageContext;
import org.opengrid.data.GenericRetrievable;
import org.opengrid.exception.ServiceException;
import org.opengrid.security.RoleAccessValidator;
import org.opengrid.security.TokenAuthenticationService;
import org.opengrid.security.TokenHandler;
import org.opengrid.service.OpenGridService;
import org.opengrid.util.ServiceProperties;

import javax.annotation.Resource;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import javax.ws.rs.core.Response;
import org.apache.cxf.common.util.StringUtils;
import org.apache.cxf.rs.security.cors.CorsHeaderConstants;
import org.opengrid.cache.CacheService;
import org.opengrid.data.ServiceCapabilities;
import org.opengrid.data.meta.OpenGridDataset;
import org.opengrid.data.meta.OpenGridMeta;
import org.opengrid.util.FileUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

@Component("OpenGridServiceBean")
public class OpenGridPlenarioService implements OpenGridService {

    private OpenGridMeta cacheMetaData;
    
    @Autowired
    @Qualifier("ehcacheServiceDatasets")
    private CacheService cacheService;

    @Resource(name="plenarioDataProvider")
    private GenericRetrievable omniDataProvider;

    //@Resource(name="queryDataProvider")
    //private Retrievable queryDataProvider;

    //@Resource(name="queryDataUpdater")
    //private Updatable queryDataUpdater;

    //@Resource(name="userDataProvider")
    //private Retrievable userDataProvider;

    //@Resource(name="userDataUpdater")
    //private Updatable userDataUpdater;

    //@Resource(name="groupDataProvider")
    //private Retrievable groupDataProvider;

    //@Resource(name="groupDataUpdater")
    //private Updatable groupDataUpdater;

    @Resource(name="roleAccessValidator")
    private RoleAccessValidator roleAccessValidator;

    @Resource(name="tokenAuthService")
    private TokenAuthenticationService tokenAuthService;

    @Override
    public void getOpenGridUserToken(final String payLoad) {
            //dummy impl to please the compiler
            //the StatelessLoginFilter class takes care of this as configured in SpringSecurityConfig
    }


    @Override
    public String addOpenGridNewQuery(String payLoad) {
      throw new ServiceException("Functionality Does Not Work With Plenario.");
    //	return queryDataUpdater.update(null, payLoad);
    }


    @Override
    public String addOpenGridNewUser(String payLoad) {
            throw new ServiceException("Functionality Does Not Work With Plenario.");
            //return userDataUpdater.update(null, payLoad);
    }


    @Override
    public void deleteOpenGridQuery(String queryId) {
            throw new ServiceException("Functionality Does Not Work With Plenario.");
        //queryDataUpdater.delete(queryId);
    }


    @Override
    public void deleteOpenGridUser(String userId) {
        throw new ServiceException("Functionality Does Not Work With Plenario.");
            //userDataUpdater.delete(userId);
    }


    @Override
    public String executeOpenGridQueryWithParams(String datasetId, String filter, int max, String sort, String options) {
        OpenGridMeta meta = new OpenGridMeta();
        if(cacheMetaData != null) {
            meta = cacheMetaData;
        }
        else
        {
            meta = omniDataProvider.getAllDatasts();
            cacheMetaData = meta;
        }
        return omniDataProvider.getData(
                            datasetId,
                            meta,
                            filter,
                            max,
                            sort,
                            options);
    }


    @Override
    public String getOpenGridDataset(String datasetId) throws JsonParseException, JsonMappingException, ServiceException, IOException {
        OpenGridMeta meta = new OpenGridMeta();
        if(cacheMetaData != null) {
            meta = cacheMetaData;
        }
        else
        {
            meta = omniDataProvider.getAllDatasts();
            cacheMetaData = meta;
        }
        
        return omniDataProvider.getDescriptor(
                            meta,
                            datasetId).toString();
    }


    @Override
    public String getOpenGridDatasetList(MessageContext mc) throws JsonParseException, JsonMappingException, ServiceException, IOException {
        String key = ServiceProperties.getProperties().getStringProperty("auth.key");
        tokenAuthService.setKey(key);
        String token = tokenAuthService.getToken(mc.getHttpServletRequest());

        TokenHandler h = new TokenHandler();
        h.setSecret(key);
        Claims c = h.getClaims(token);
        String descriptors = "";
        OpenGridMeta meta = new OpenGridMeta();

        if(cacheMetaData != null) {
            meta = cacheMetaData;
        }
        else
        {
            meta = omniDataProvider.getAllDatasts();
            cacheMetaData = meta;
        }
        
        if(cacheService.get(DATASETS_CHACHE_KEY) != null) {
            descriptors = (String)cacheService.get(DATASETS_CHACHE_KEY);
        }
        if(descriptors == null || descriptors.isEmpty()){
            for (OpenGridDataset s: meta.getDatasets()) {
                if (roleAccessValidator.lookupAccessMap("/rest/datasets/" + s, "GET", (String) c.get("resources"))) {
                        if (descriptors != null && !descriptors.isEmpty())
                                descriptors += ", ";
                        descriptors += s.toString();
                }
            }
            if(descriptors != null && !descriptors.isEmpty()){
                cacheService.put(DATASETS_CHACHE_KEY, descriptors);
            }
        }

        return "[" + descriptors + "]";
    }


    @Override
    public String getOpenGridQueriesList(String filter, int max, String sort) {
        return FileUtil.getJsonFileContents("json/PlenarioCommonQueries.json");
        //return omniDataProvider.getCommonQueries();//queryDataProvider.getData(filter, max, sort);
    }


    @Override
    public String getOpenGridQuery(String queryId) {
        throw new ServiceException("Functionality Does Not Work With Plenario.");
            //String filter = "{\"_id\": {\"$eq\": " + queryId + "}}";
            //return queryDataProvider.getData(filter, 1, null);
    }


    @Override
    public String getOpenGridUser(String userId) {
        throw new ServiceException("Functionality Does Not Work With Plenario.");
            //String filter = "{\"_id\": {\"$eq\": " + userId + "}}";
            //return userDataProvider.getData(filter, 1, null);
    }


    @Override
    public String getOpenGridUsersList(String filter, int max, String sort) {
        //throw new ServiceException("Functionality Does Not Work With Plenario.");
        return "[]";
    }


    @Override
    public String updateOpenGridQuery(String queryId, String payLoad) {
        throw new ServiceException("Functionality Does Not Work With Plenario.");
        //return queryDataUpdater.update(queryId, payLoad);
    }


    @Override
    public String updateOpenGridUser(String userId, String payLoad) {
        throw new ServiceException("Functionality Does Not Work With Plenario.");
        //return userDataUpdater.update(userId, payLoad);
    }


    @Override
    public String getOpenGridGroupsList(String filter, int max, String sort) {
        //return groupDataProvider.getData(filter, max, sort);
        return "[\"opengrid_users\",\"opengrid_admins\"]";
    }


    @Override
    public String getOpenGridGroup(String groupId) {
        //String filter = "{\"_id\": {\"$eq\": " + groupId + "}}";
        //return groupDataProvider.getData(filter, 1, null);
        return "[]";
    }


    @Override
    public String addOpenGridNewGroup(String payLoad) {
        throw new ServiceException("Functionality Does Not Work With Plenario.");
        //return groupDataUpdater.update(null, payLoad);
    }


    @Override
    public String updateOpenGridgroup(String groupId, String payLoad) {
        throw new ServiceException("Functionality Does Not Work With Plenario.");
        //return groupDataUpdater.update(groupId, payLoad);
    }


    @Override
    public void deleteOpenGridGroup(String groupId) {
        throw new ServiceException("Functionality Does Not Work With Plenario.");
        //groupDataUpdater.delete(groupId);
    }


    @Override
    public void refreshOpenGridUserToken(MessageContext mc) {
            tokenAuthService.setKey(ServiceProperties.getProperties().getStringProperty("auth.key"));
            tokenAuthService.renewAuthentication(mc.getHttpServletRequest(), mc.getHttpServletResponse());
    }

    /**
     * @param cacheService the cacheService to set
     */
    public void setCacheService(CacheService cacheService) {
            this.cacheService = cacheService;
    }
    
    @Override
    public ServiceCapabilities getServiceCapabilities() {
            //customize this depending on what this service implementation support
            ServiceCapabilities sc = new ServiceCapabilities();

            sc.setGeoSpatialFiltering(true);
            return sc;
    }
    
        @Override
    public Response options() {
        return Response.ok()
                    .header(CorsHeaderConstants.HEADER_AC_ALLOW_HEADERS, "origin, content-type, accept, authorization, X-AUTH-TOKEN")
                    .header(CorsHeaderConstants.HEADER_AC_ALLOW_METHODS, "GET, POST, PUT, DELETE, OPTIONS, HEAD")
           //.header(CorsHeaderConstants.HEADER_AC_ALLOW_CREDENTIALS, "false")
           //.header(CorsHeaderConstants.HEADER_AC_ALLOW_ORIGIN, "*")
           .header(CorsHeaderConstants.HEADER_AC_EXPOSE_HEADERS, "X-AUTH-TOKEN")
           .build();       
    }
    
    @Override
 	public String executeOpenGridQueryWithParamsPost(String datasetId, String filter, int max, String sort, String options) {
 		//this is called when POST method is used
 		//we have now transitioned to using POST as of core 1.3.0
 		return executeOpenGridQueryWithParams(datasetId, filter, max, sort, options);
 	}

}
