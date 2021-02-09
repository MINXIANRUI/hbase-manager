package com.leo.hbase.manager.web.service.impl;

import com.github.CCweixiao.HBaseSqlTemplate;
import com.github.CCweixiao.HBaseTemplate;
import com.github.CCwexiao.dsl.client.HBaseCellResult;
import com.github.CCwexiao.dsl.config.HBaseTableConfig;
import com.leo.hbase.manager.web.hds.HBaseClusterDSConfig;
import com.leo.hbase.manager.web.service.IMultiHBaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author leojie 2020/9/24 9:46 下午
 */
@Service
public class MultiHBaseServiceImpl implements IMultiHBaseService {
    @Autowired
    private HBaseClusterDSConfig hBaseClusterDSConfig;

    @Override
    public List<Map<String, Object>> get(String clusterCode, String tableName, String rowKey, String familyName) {
        HBaseTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseTemplate(clusterCode);

        return hBaseTemplate.getToListMapWithFamily(tableName, rowKey, familyName);
    }

    @Override
    public Map<String, Object> get(String clusterCode, String tableName, String rowKey, String familyName, String qualifier) {
        HBaseTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseTemplate(clusterCode);
        return hBaseTemplate.getToMapWithFamilyAndQualifier(tableName, rowKey, familyName, Collections.singletonList(qualifier));
    }

    @Override
    public List<List<Map<String, Object>>> find(String clusterCode, String tableName, String familyName, String startKey, Integer limit) {
        HBaseTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseTemplate(clusterCode);
        return hBaseTemplate.findToListMap(tableName, familyName, startKey, limit);
    }

    @Override
    public void delete(String clusterCode, String tableName, String rowKey, String familyName, String qualifier) {
        HBaseTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseTemplate(clusterCode);
        hBaseTemplate.delete(tableName, rowKey, familyName, qualifier);
    }

    @Override
    public void saveOrUpdate(String clusterCode, String tableName, String rowKey, String familyAndQualifierName, String value) {
        HBaseTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseTemplate(clusterCode);
        Map<String, Object> data = new HashMap<>(1);
        data.put(familyAndQualifierName, value);
        hBaseTemplate.save(tableName, rowKey, data);
    }

    @Override
    public List<List<HBaseCellResult>> select(String clusterCode, HBaseTableConfig hBaseTableConfig, String hql) {
        HBaseSqlTemplate hBaseSqlTemplate = hBaseClusterDSConfig.getHBaseSqlTemplate(clusterCode);
        hBaseSqlTemplate.setHBaseTableConfig(hBaseTableConfig);
        return hBaseSqlTemplate.select(hql);
    }

    @Override
    public void insert(String clusterCode, HBaseTableConfig hBaseTableConfig, String hql) {
        HBaseSqlTemplate hBaseSqlTemplate = hBaseClusterDSConfig.getHBaseSqlTemplate(clusterCode);
        hBaseSqlTemplate.setHBaseTableConfig(hBaseTableConfig);
        hBaseSqlTemplate.insert(hql);
    }

    @Override
    public void delete(String clusterCode, HBaseTableConfig hBaseTableConfig, String hql) {
        HBaseSqlTemplate hBaseSqlTemplate = hBaseClusterDSConfig.getHBaseSqlTemplate(clusterCode);
        hBaseSqlTemplate.setHBaseTableConfig(hBaseTableConfig);
        hBaseSqlTemplate.delete(hql);
    }
}
