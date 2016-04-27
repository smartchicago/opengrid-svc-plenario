package org.opengrid.cache;

import java.io.Serializable;

/**
 * Basic abstraction of Cache providers to allow for varying Cache provider
 * implementations.
 *  
 * @author Mohan Pittala
 * 
 */

public interface CacheService {


    /**
     * Checks if item is populated in the cache.
     */
    public boolean contains(Serializable key);

    /**
     * Retrieves item form cache.
     */
    public Serializable get(Serializable key);

    /**
     * Puts item in cache.
     */
    public void put(Serializable key, Serializable value);

    /**
     * Puts item in cache.
     */
    public void put(Object key, Object value);

     /**
     * Returns entry that was removed, or null if it did not exist.
     */
    public boolean remove(Serializable key);

    /**
     * Returns implementation specific cache provider.
     */
    public Object getCacheProvider();
	    
}

	

