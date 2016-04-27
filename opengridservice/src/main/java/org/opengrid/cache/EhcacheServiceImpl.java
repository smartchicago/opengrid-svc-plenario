package org.opengrid.cache;

import java.io.Serializable;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Ehcache implementation of Cache. If used, the ehcache dependency must be
 * specified.
 * 
 * @author Mohan Pittala
 *
 */
public class EhcacheServiceImpl implements CacheService
{
    private static final Log _log = LogFactory.getLog(EhcacheServiceImpl.class.getName());

    private String cacheName;
    
    private CacheManager cacheManager; 

    /**
     * @return String.
     */
    public String getCacheName(){
            return cacheName;
    }

    /**
     * @param cacheName The cacheName to set.
     */
    public void setCacheName(String cacheName) {
            this.cacheName = cacheName;
    }

    /**
     * @return Returns the cacheManager.
     */
    public CacheManager getCacheManager() {
            return cacheManager;
    }

    /**
     * @param cacheManager The cacheManager to set.
     */
    public void setCacheManager(CacheManager cacheManager) {
            this.cacheManager = cacheManager;
    }


    /**
     * Initializes cache with given cache name. Defaults to <code>enabled</code>.
     */
    public EhcacheServiceImpl(String cacheName, CacheManager cacheManager)
    {
        this.cacheName = cacheName;
        this.cacheManager = cacheManager;
        Cache cache = getCache();
        if (cache == null) {
            _log.info("Cache failed to initialize: " + getCacheName());
        } else {
            _log.info("Cache initialized: " + getCacheName());
        }            
    }

    public boolean contains(Serializable key)
    {
        return (get(key) != null);
    }

    public Serializable get(Serializable key)
    {
        Serializable value = null;

        try
        {
            Cache cache = getCache();
            Element element = cache.get(key);

            if (element != null)
                value = element.getValue();
        }
        catch (Throwable ex)
        {
            _log.error("get(Serializable) [cache: " + getCacheName() + "] - ex: " + ex);
        }

        return value;
    }

    public void put(Serializable key, Serializable value)
    {
        try
        {
            getCache().put(new Element(key, value));
        }
        catch (Throwable ex)
        {
            _log.error("put(Serializable, Serializable) [cache: " + getCacheName() + "] - ex: " + ex);
        }
    }
    
    public void put(Object key, Object value)
    {
        try
        {

            getCache().put(new Element(key, value));
        }
        catch (Throwable ex)
        {
            _log.error("put(Object, Object) [cache: " + getCacheName() + "] - ex: " + ex);
        }
    }
    
    public boolean remove(Serializable key)
    {
        try
        {
            return getCache().remove(key);
        }
        catch (Throwable ex)
        {
            _log.error("remove(Serializable) [cache: " + getCacheName() + "] - ex: " + ex);
        }

        return false;
    }

    public Object getCacheProvider()
    {
        return getCache();
    }

    protected Cache getCache()
    {
        Cache cache = null;

        try
        {
            cache = getCacheManager().getCache(getCacheName());
        }
        catch (Throwable ex)
        {
            _log.error("getCache(String cacheName)) - ex: " + ex);
        }

        return cache;
    }
}
