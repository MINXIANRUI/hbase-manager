package com.leo.hbase.manager.web.service.impl;

import com.github.CCweixiao.HBaseAdminTemplate;
import com.github.CCweixiao.constant.HMHBaseConstant;
import com.github.CCweixiao.exception.HBaseOperationsException;
import com.github.CCweixiao.hbtop.Record;
import com.github.CCweixiao.hbtop.RecordFilter;
import com.github.CCweixiao.hbtop.Summary;
import com.github.CCweixiao.hbtop.field.Field;
import com.github.CCweixiao.hbtop.mode.Mode;
import com.github.CCweixiao.model.ColumnFamilyDesc;
import com.github.CCweixiao.model.HTableDesc;
import com.github.CCweixiao.model.NamespaceDesc;
import com.github.CCweixiao.model.SnapshotDesc;
import com.github.CCweixiao.util.SplitGoEnum;
import com.leo.hbase.manager.common.constant.HBasePropertyConstants;
import com.leo.hbase.manager.common.exception.BusinessException;
import com.leo.hbase.manager.common.utils.HBaseConfigUtils;
import com.leo.hbase.manager.common.utils.StringUtils;
import com.leo.hbase.manager.common.utils.security.StrEnDeUtils;
import com.leo.hbase.manager.framework.util.ShiroUtils;
import com.leo.hbase.manager.system.domain.SysUserHbaseTable;
import com.leo.hbase.manager.system.mapper.SysUserHbaseTableMapper;
import com.leo.hbase.manager.web.hds.HBaseClusterDSConfig;
import com.leo.hbase.manager.web.service.IMultiHBaseAdminService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author leojie 2020/9/24 9:47 下午
 */
@Service
public class MultiHBaseAdminServiceImpl implements IMultiHBaseAdminService {
    @Autowired
    HBaseClusterDSConfig hBaseClusterDSConfig;

    @Autowired
    private SysUserHbaseTableMapper userHbaseTableMapper;

    @Override
    public NamespaceDesc getNamespaceDesc(String clusterCode, String namespaceName) {
        final String filterNamespacePrefix = HBaseConfigUtils.getFilterNamespacePrefix(clusterCode);
        if (StringUtils.isNotBlank(filterNamespacePrefix)) {
            if (namespaceName.startsWith(filterNamespacePrefix)) {
                throw new BusinessException("已配置过滤该命名空间的前缀");
            }
        }

        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.getNamespaceDesc(namespaceName);
    }

    @Override
    public List<NamespaceDesc> listAllNamespaceDesc(String clusterCode) {
        final String filterNamespacePrefix = HBaseConfigUtils.getFilterNamespacePrefix(clusterCode);
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        final List<NamespaceDesc> namespaceDescList = hBaseTemplate.listNamespaceDesc();
        if (StringUtils.isNotBlank(filterNamespacePrefix)) {
            return namespaceDescList.stream().filter(namespaceDesc -> !namespaceDesc.getNamespaceName().startsWith(filterNamespacePrefix)).collect(Collectors.toList());
        }
        return namespaceDescList;
    }

    @Override
    public List<String> listAllNamespaceName(String clusterCode) {
        final String filterNamespacePrefix = HBaseConfigUtils.getFilterNamespacePrefix(clusterCode);
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        final List<String> namespaces = hBaseTemplate.listNamespaceNames();

        if (StringUtils.isNotBlank(filterNamespacePrefix)) {
            return namespaces.stream().filter(namespace -> !namespace.startsWith(filterNamespacePrefix)).collect(Collectors.toList());
        }
        return namespaces;
    }

    @Override
    public List<String> listAllTableNamesByNamespaceName(String clusterCode, String namespaceName) {
        final String filterNamespacePrefix = HBaseConfigUtils.getFilterNamespacePrefix(clusterCode);
        if (StringUtils.isNotBlank(filterNamespacePrefix)) {
            if (namespaceName.startsWith(filterNamespacePrefix)) {
                throw new BusinessException("已配置过滤该命名空间的前缀");
            }
        }
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.listTableNamesByNamespace(namespaceName);
    }

    @Override
    public boolean createNamespace(String clusterCode, NamespaceDesc namespace) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.createNamespace(namespace, true);
    }

    @Override
    public boolean deleteNamespace(String clusterCode, String namespace) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.deleteNamespace(namespace, true);
    }

    @Override
    public List<String> listAllTableName(String clusterCode, boolean checkAuth) {
        final String filterNamespacePrefix = HBaseConfigUtils.getFilterNamespacePrefix(clusterCode);
        final String filterTableNamePrefix = HBaseConfigUtils.getFilterTableNamePrefix(clusterCode);

        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        List<String> tableNames = hBaseTemplate.listTableNames();

        if (checkAuth) {
            Long userId = ShiroUtils.getUserId();
            final List<SysUserHbaseTable> sysUserHbaseTables = userHbaseTableMapper.selectSysUserHbaseTableListByUserAndClusterAlias(userId, clusterCode);
            if (sysUserHbaseTables == null || sysUserHbaseTables.isEmpty()) {
                return new ArrayList<>();
            }

            final List<String> authTableNames = sysUserHbaseTables.stream().map(SysUserHbaseTable::getTableName).collect(Collectors.toList());
            tableNames = tableNames.stream().filter(tableName -> authTableNames.contains(HMHBaseConstant.getFullTableName(tableName)))
                    .collect(Collectors.toList());

        }

        if (StringUtils.isNotBlank(filterNamespacePrefix) && StringUtils.isNotBlank(filterTableNamePrefix)) {
            return tableNames.stream().filter(tableName -> {
                final String namespaceName = HMHBaseConstant.getNamespaceName(tableName);
                return !namespaceName.startsWith(filterNamespacePrefix);
            }).filter(tableName -> {
                String shortTableName = HMHBaseConstant.getFullTableName(tableName).split(HMHBaseConstant.TABLE_NAME_SPLIT_CHAR)[1];
                return !shortTableName.startsWith(filterTableNamePrefix);
            }).collect(Collectors.toList());

        } else if (StringUtils.isNotBlank(filterNamespacePrefix) && StringUtils.isBlank(filterTableNamePrefix)) {
            return tableNames.stream().filter(tableName -> {
                final String namespaceName = HMHBaseConstant.getNamespaceName(tableName);
                return !namespaceName.startsWith(filterNamespacePrefix);
            }).collect(Collectors.toList());

        } else if (StringUtils.isBlank(filterNamespacePrefix) && StringUtils.isNotBlank(filterTableNamePrefix)) {
            return tableNames.stream().filter(tableName -> {
                String shortTableName = HMHBaseConstant.getFullTableName(tableName).split(HMHBaseConstant.TABLE_NAME_SPLIT_CHAR)[1];
                return !shortTableName.startsWith(filterTableNamePrefix);
            }).collect(Collectors.toList());
        }
        return tableNames;
    }

    @Override
    public List<HTableDesc> listAllHTableDesc(String clusterCode, boolean checkAuth) {
        final String filterNamespacePrefix = HBaseConfigUtils.getFilterNamespacePrefix(clusterCode);
        final String filterTableNamePrefix = HBaseConfigUtils.getFilterTableNamePrefix(clusterCode);

        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        List<HTableDesc> tableDescList = hBaseTemplate.listTableDesc();

        if (checkAuth) {
            Long userId = ShiroUtils.getUserId();
            final List<SysUserHbaseTable> sysUserHbaseTables = userHbaseTableMapper.selectSysUserHbaseTableListByUserAndClusterAlias(userId, clusterCode);
            if (sysUserHbaseTables == null || sysUserHbaseTables.isEmpty()) {
                return new ArrayList<>();
            }

            final List<String> authTableNames = sysUserHbaseTables.stream().map(SysUserHbaseTable::getTableName).collect(Collectors.toList());
            tableDescList = tableDescList.stream().filter(tableDesc -> authTableNames.contains(tableDesc.getFullTableName()))
                    .collect(Collectors.toList());

        }

        if (StringUtils.isNotBlank(filterNamespacePrefix) && StringUtils.isNotBlank(filterTableNamePrefix)) {
            return tableDescList.stream().filter(tableDesc -> !tableDesc.getNamespaceName().startsWith(filterNamespacePrefix))
                    .filter(tableDesc -> {
                        String shortTableName = HMHBaseConstant.getFullTableName(tableDesc.getTableName()).split(HMHBaseConstant.TABLE_NAME_SPLIT_CHAR)[1];
                        return !shortTableName.startsWith(filterTableNamePrefix);
                    }).collect(Collectors.toList());

        } else if (StringUtils.isNotBlank(filterNamespacePrefix) && StringUtils.isBlank(filterTableNamePrefix)) {
            return tableDescList.stream().filter(tableDesc -> !tableDesc.getNamespaceName().startsWith(filterNamespacePrefix)).collect(Collectors.toList());

        } else if (StringUtils.isBlank(filterNamespacePrefix) && StringUtils.isNotBlank(filterTableNamePrefix)) {
            return tableDescList.stream().filter(tableDesc -> {
                String shortTableName = HMHBaseConstant.getFullTableName(tableDesc.getTableName()).split(HMHBaseConstant.TABLE_NAME_SPLIT_CHAR)[1];
                return !shortTableName.startsWith(filterTableNamePrefix);
            }).collect(Collectors.toList());
        }
        return tableDescList;
    }

    @Override
    public List<SnapshotDesc> listAllSnapshotDesc(String clusterCode) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        List<SnapshotDesc> snapshotDescList = hBaseTemplate.listSnapshots();

        Long userId = ShiroUtils.getUserId();
        final List<SysUserHbaseTable> sysUserHbaseTables = userHbaseTableMapper.selectSysUserHbaseTableListByUserAndClusterAlias(userId, clusterCode);

        if (sysUserHbaseTables == null || sysUserHbaseTables.isEmpty()) {
            return new ArrayList<>();
        }

        final List<String> authTableNames = sysUserHbaseTables.stream().map(SysUserHbaseTable::getTableName).collect(Collectors.toList());
        snapshotDescList = snapshotDescList.stream().filter(snapshotDesc -> authTableNames.contains(HMHBaseConstant.getFullTableName(snapshotDesc.getTableName())))
                .collect(Collectors.toList());

        return snapshotDescList;
    }

    @Override
    public boolean createTable(String clusterCode, HTableDesc tableDesc) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.createTable(tableDesc);
    }

    @Override
    public boolean createTable(String clusterCode, HTableDesc tableDesc, String startKey, String endKey, int numRegions, boolean isAsync) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.createTable(tableDesc, startKey, endKey, numRegions, isAsync);

    }

    @Override
    public boolean createTable(String clusterCode, HTableDesc tableDesc, String[] splitKeys, boolean isAsync) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.createTable(tableDesc, splitKeys, isAsync);
    }

    @Override
    public boolean createSnapshot(String clusterCode, SnapshotDesc snapshotDesc) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.snapshot(snapshotDesc, true);
    }

    @Override
    public boolean removeSnapshot(String clusterCode, String snapshotName) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.deleteSnapshot(snapshotName);
    }

    @Override
    public boolean createTable(String clusterCode, HTableDesc tableDesc, SplitGoEnum splitGoEnum, int numRegions, boolean isAsync) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.createTable(tableDesc, splitGoEnum, numRegions, isAsync) ;
    }

    @Override
    public boolean enableTable(String clusterCode, String tableName) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.enableTableAsync(tableName);
    }

    @Override
    public boolean disableTable(String clusterCode, String tableName) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.disableTableAsync(tableName);
    }

    @Override
    public boolean isTableDisabled(String clusterCode, String tableName) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.isTableDisabled(tableName);
    }

    @Override
    public boolean tableIsExists(String clusterCode, String tableName) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.tableExists(tableName);
    }

    @Override
    public boolean deleteTable(String clusterCode, String tableName) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.deleteTable(tableName, true);
    }

    @Override
    public boolean truncatePreserve(String clusterCode, String tableName) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.truncateTable(tableName, true, true);
    }

    @Override
    public HTableDesc getHTableDesc(String clusterCode, String tableName) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.getTableDesc(tableName);
    }

    @Override
    public List<ColumnFamilyDesc> getColumnFamilyDesc(String clusterCode, String tableName) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.listFamilyDesc(tableName);
    }

    @Override
    public boolean addFamily(String clusterCode, String tableName, ColumnFamilyDesc familyDesc) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.addFamily(tableName, familyDesc, true);
    }

    @Override
    public boolean deleteFamily(String clusterCode, String tableName, String familyName) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.deleteFamily(tableName, familyName, true);
    }

    @Override
    public boolean modifyFamily(String clusterCode, String tableName, ColumnFamilyDesc familyDesc) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.modifyFamily(tableName, familyDesc, true);
    }

    @Override
    public boolean enableReplication(String clusterCode, String tableName, List<String> families) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.enableReplicationScope(tableName, families, true);
    }

    @Override
    public boolean disableReplication(String clusterCode, String tableName, List<String> families) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.disableReplicationScope(tableName, families, true);
    }

    @Override
    public boolean modifyTable(String clusterCode, HTableDesc tableDesc) {
        //todo modifyTable
        return true;
    }

    @Override
    public boolean modifyTableProps(String clusterCode, HTableDesc tableDesc) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.modifyTablePropsAsync(tableDesc.getFullTableName(), tableDesc.getTableProps());
    }

    @Override
    public int totalHRegionServerNum(String clusterCode) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);

        return 5;
    }

    @Override
    public int totalNamespaceNum(String clusterCode) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.listNamespaceNames().size();
    }

    @Override
    public int totalTableNum(String clusterCode) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.listTableNames().size();
    }

    @Override
    public int totalSnapshotNum(String clusterCode) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        final List<SnapshotDesc> snapshots = hBaseTemplate.listSnapshots();
        if (snapshots == null || snapshots.isEmpty()) {
            return 0;
        } else {
            return snapshots.size();
        }
    }

    @Override
    public Summary refreshSummary(String clusterCode) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.refreshSummary();
    }

    @Override
    public List<Record> refreshRecords(String clusterCode, Mode currentMode, List<RecordFilter> filters, Field currentSortField, boolean ascendingSort) {
        HBaseAdminTemplate hBaseTemplate = hBaseClusterDSConfig.getHBaseAdminTemplate(clusterCode);
        return hBaseTemplate.refreshRecords(currentMode, filters, currentSortField, ascendingSort);
    }


    @Transactional(rollbackFor = HBaseOperationsException.class)
    public void deleteUserTableRelation(String tableName, String clusterCode) {
        tableName = HMHBaseConstant.getFullTableName(tableName);
        SysUserHbaseTable sysUserHbaseTable = new SysUserHbaseTable();
        sysUserHbaseTable.setUserId(ShiroUtils.getUserId());
        sysUserHbaseTable.setTableName(tableName);
        sysUserHbaseTable.setTableId(StrEnDeUtils.encrypt(tableName));
        sysUserHbaseTable.setClusterAlias(clusterCode);
        userHbaseTableMapper.deleteSysUserHbaseTable(sysUserHbaseTable);
    }

}
