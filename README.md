# Cache-Burger

* Cache manager to read/write local and remote cache distributed automatically.
* You can use a cache more efficient than local cache, more high performance than remote cache.
* Asynchronous and non blocking update cache data between local and remote cache by RxJava.
* Support to query cache hit rate

----------



### Bean properties

~~~ xml
    <bean name="modelCacheDefinition" class="com.midday.burger.cacheburger.model.BurgerCacheDefinition">
    	<!-- name of cache -->
        <property name="name" value="example" />
        <!-- cache class model -->
        <property name="cacheModelClass" value="com.midday.project.SomeClass" />
        <!-- LocalCache (EhCache) Max Element -->
        <property name="localCacheElementMaxCount" value="100000" />
        <!-- LocalCache (EhCache) Expire second -->
        <property name="localCacheTime" value="20" />
        <!-- RemoteCache (Couchbase) Server adddress -->
        <property name="couchbaseNode" value="http://couchbase-server.company.com:8091" />
        <!-- RemoteCache (Couchbase) Bucket name -->
        <property name="couchbaseBucket" value="bucket" />
        <!-- RemoteCache (Couchbase) Bucket password -->
        <property name="couchbasePassword" value="password" />
        <!-- RemoteCache (Couchbase) TTL -->
        <property name="remoteCacheTime" value="300" />
    </bean>

    <bean name="modelDataCache" class="com.midday.burger.cacheburger.BurgerCacheManager">
        <property name="cacheDefinition" ref="modelCacheDefinition" />
    	<!-- TTL time for asynchronos and non blocking update of remote cache.-->
        <!-- If 0, caches are updated with blocking method -->
        <property name="remoteCacheAsyncTime" value="60" />
        <property name="asyncThreadCount" value="1" />
    </bean>
~~~
----------
### Quick and easy example


- Get and update if expired or too long
~~~ java
<<<<<<< HEAD
modelDataCache.get(dataNo, data -> getData(data));
modelDataCache.gets(dataNo, dataList -> getData(new ArrayList<>(dataList)));
=======
 modelDataCache.gets(dataNo, dataList -> getData(new ArrayList<>(dataList)));
>>>>>>> master
~~~

- Update to local and remote cache automatically
~~~ java
<<<<<<< HEAD
modelDataCache.updateCache(dataNo, data);
=======
 modelDataCache.updateCache(dataNo, data);
>>>>>>> master
~~~
