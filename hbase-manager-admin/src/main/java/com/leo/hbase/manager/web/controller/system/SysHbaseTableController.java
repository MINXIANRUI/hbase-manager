package com.leo.hbase.manager.web.controller.system;

import com.alibaba.fastjson.JSON;
import com.github.CCweixiao.constant.HMHBaseConstant;
import com.github.CCweixiao.model.ColumnFamilyDesc;
import com.github.CCweixiao.model.HTableDesc;
import com.github.CCweixiao.model.NamespaceDesc;
import com.github.CCweixiao.util.SplitGoEnum;
import com.github.CCweixiao.util.StrUtil;
import com.leo.hbase.manager.common.annotation.Log;
import com.leo.hbase.manager.common.constant.HBaseManagerConstants;
import com.leo.hbase.manager.common.core.domain.AjaxResult;
import com.leo.hbase.manager.common.core.domain.CxSelect;
import com.leo.hbase.manager.common.core.page.TableDataInfo;
import com.leo.hbase.manager.common.core.text.Convert;
import com.leo.hbase.manager.common.enums.BusinessType;
import com.leo.hbase.manager.common.utils.poi.ExcelUtil;
import com.leo.hbase.manager.common.utils.security.StrEnDeUtils;
import com.leo.hbase.manager.framework.util.ShiroUtils;
import com.leo.hbase.manager.system.domain.SysHbaseTag;
import com.leo.hbase.manager.system.domain.SysUserHbaseTable;
import com.leo.hbase.manager.system.dto.NamespaceDescDto;
import com.leo.hbase.manager.system.dto.TableDescDto;
import com.leo.hbase.manager.system.service.ISysUserHbaseTableService;
import com.leo.hbase.manager.system.service.ISysUserService;
import com.leo.hbase.manager.web.controller.query.QueryHBaseTableForm;
import com.leo.hbase.manager.web.service.IMultiHBaseAdminService;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.leo.hbase.manager.common.constant.HBasePropertyConstants.HBASE_TABLE_DISABLE_FLAG;
import static com.leo.hbase.manager.common.constant.HBasePropertyConstants.HBASE_TABLE_ENABLE_FLAG;

/**
 * HBaseController
 *
 * @author leojie
 * @date 2020-08-16
 */
@Controller
@RequestMapping("/system/table")
public class SysHbaseTableController extends SysHbaseBaseController {
    private String prefix = "system/table";

    @Autowired
    private IMultiHBaseAdminService multiHBaseAdminService;

    @Autowired
    private ISysUserHbaseTableService userHbaseTableService;

    @Autowired
    private ISysUserService userService;


    @RequiresPermissions("hbase:table:view")
    @GetMapping()
    public String table(ModelMap mmap) {
        final List<NamespaceDescDto> namespaceDescList = getAllNamespaces();
        mmap.put("namespaces", namespaceDescList);
        mmap.put("tags", sysHbaseTagService.selectAllSysHbaseTagList());
        return prefix + "/table";
    }

    @GetMapping("/data/{tableId}")
    public String data(@PathVariable("tableId") String tableId, ModelMap mmap) {
        final String tableName = parseTableNameFromTableId(tableId);
        mmap.put("tableFamilyData", JSON.toJSON(getTableSelectData(tableName)));
        mmap.put("tableId", tableId);
        return prefix + "/data";
    }
    @GetMapping("/data/{tableId}/add")
    public String addData(@PathVariable("tableId") String tableId, ModelMap mmap) {
        final String tableName = parseTableNameFromTableId(tableId);
        mmap.put("tableFamilyData", JSON.toJSON(getTableSelectData(tableName)));
        return prefix + "/addData";
    }


    @RequiresPermissions("hbase:table:detail")
    @GetMapping("/detail/{tableId}")
    public String detail(@PathVariable("tableId") String tableId, ModelMap mmap) {
        final String tableName = parseTableNameFromTableId(tableId);
        final HTableDesc tableDesc = multiHBaseAdminService.getHTableDesc(clusterCodeOfCurrentSession(), tableName);
        TableDescDto tableDescDto = new TableDescDto().convertFor(tableDesc);
        tableDescDto.setSysHbaseTagList(getSysHbaseTagByLongIds(tableDescDto.getTagIds()));

        mmap.put("hbaseTable", tableDescDto);
        return prefix + "/detail";
    }

    @RequiresPermissions("hbase:table:detail")
    @GetMapping("/family/detail/{tableId}")
    public String familyDetail(@PathVariable("tableId") String tableId, ModelMap mmap) {
        final String tableName = StrEnDeUtils.decrypt(tableId);

        String clusterCode = clusterCodeOfCurrentSession();
        HTableDesc tableDesc = multiHBaseAdminService.getHTableDesc(clusterCode, tableName);
        TableDescDto tableDescDto = new TableDescDto().convertFor(tableDesc);
        mmap.put("tableObj", tableDescDto);

        final List<String> listAllTableName = multiHBaseAdminService.listAllTableName(clusterCode, true);

        if (listAllTableName == null || listAllTableName.isEmpty()) {
            mmap.put("tableMapList", new ArrayList<>());
        } else {
            List<Map<String, Object>> tableMapList = new ArrayList<>(listAllTableName.size());
            for (String tableName_ : listAllTableName) {
                Map<String, Object> tableMap = new HashMap<>(2);
                tableMap.put("tableId", StrEnDeUtils.encrypt(tableName_));
                tableMap.put("tableName", tableName_);
                tableMapList.add(tableMap);
                mmap.put("tableMapList", tableMapList);
            }
        }
        return "system/family/family";
    }

    /**
     * ??????HBase??????
     */
    @RequiresPermissions("hbase:table:list")
    @PostMapping("/list")
    @ResponseBody
    public TableDataInfo list(QueryHBaseTableForm queryHBaseTableForm) {
        final List<HTableDesc> tableDescList = multiHBaseAdminService.listAllHTableDesc(clusterCodeOfCurrentSession(), true);
        final List<TableDescDto> tableDescDtoList = filterFamilyDescList(queryHBaseTableForm, tableDescList);
        return getDataTable(tableDescDtoList);
    }

    /**
     * ??????HBase??????
     */
    @RequiresPermissions("hbase:table:export")
    @Log(title = "HBase???????????????", businessType = BusinessType.EXPORT)
    @PostMapping("/export")
    @ResponseBody
    public AjaxResult export(QueryHBaseTableForm queryHBaseTableForm) {
        final List<HTableDesc> tableDescList = multiHBaseAdminService.listAllHTableDesc(clusterCodeOfCurrentSession(), true);
        final List<TableDescDto> tableDescDtoList = filterFamilyDescList(queryHBaseTableForm, tableDescList);
        ExcelUtil<TableDescDto> util = new ExcelUtil<>(TableDescDto.class);
        return util.exportExcel(tableDescDtoList, "table");
    }

    /**
     * ??????HBase???
     */
    @GetMapping("/add")
    public String add(ModelMap mmap) {
        List<NamespaceDescDto> namespaces = multiHBaseAdminService.listAllNamespaceDesc(clusterCodeOfCurrentSession()).stream()
                .filter(namespaceDesc -> !HMHBaseConstant.DEFAULT_SYS_TABLE_NAMESPACE.equals(namespaceDesc.getNamespaceName()))
                .map(namespaceDesc -> new NamespaceDescDto().convertFor(namespaceDesc)).collect(Collectors.toList());
        mmap.put("namespaces", namespaces);
        mmap.put("tags", sysHbaseTagService.selectAllSysHbaseTagList());
        return prefix + "/add";
    }

    /**
     * ????????????HBase
     */
    @RequiresPermissions("hbase:table:add")
    @Log(title = "HBase?????????", businessType = BusinessType.INSERT)
    @PostMapping("/add")
    @ResponseBody
    public AjaxResult addSave(@Validated TableDescDto tableDescDto) {
        String clusterCode = clusterCodeOfCurrentSession();
        final String tableName = tableDescDto.getTableName();
        final String fullTableName = HMHBaseConstant.getFullTableName(tableDescDto.getNamespaceId(), tableName);
        final String tableId = getTableIdByName(fullTableName);
        if (multiHBaseAdminService.tableIsExists(clusterCode, fullTableName)) {
            return error("HBase???[" + fullTableName + "]???????????????");
        }
        tableDescDto.setTableName(fullTableName);
        tableDescDto.setCreateBy(ShiroUtils.getLoginName());
        tableDescDto.setCreateTimestamp(System.currentTimeMillis());
        tableDescDto.setLastUpdateBy(ShiroUtils.getLoginName());
        tableDescDto.setLastUpdateTimestamp(System.currentTimeMillis());

        HTableDesc tableDesc = tableDescDto.convertTo();
        boolean createTableRes = false;
        if (StrUtil.isBlank(tableDescDto.getSplitWay())) {
            createTableRes = multiHBaseAdminService.createTable(clusterCode, tableDesc);
        } else if (HBaseManagerConstants.SPLIT_1.equals(tableDescDto.getSplitWay())) {
            if (StrUtil.isBlank(tableDescDto.getStartKey())) {
                return error("??????????????????key????????????");
            }
            if (StrUtil.isBlank(tableDescDto.getEndKey())) {
                return error("??????????????????key????????????");
            }
            if (tableDescDto.getPreSplitRegions() < 1) {
                return error("?????????????????????0");
            }
            createTableRes = multiHBaseAdminService.createTable(clusterCode, tableDesc, tableDescDto.getStartKey(),
                    tableDescDto.getEndKey(), tableDescDto.getPreSplitRegions(), false);

        } else if (HBaseManagerConstants.SPLIT_2.equals(tableDescDto.getSplitWay())) {
            String[] splitKeys = Convert.toStrArray(tableDescDto.getPreSplitKeys());
            if (splitKeys.length < 1) {
                return error("????????????key????????????");
            }
            createTableRes = multiHBaseAdminService.createTable(clusterCode, tableDesc, splitKeys, false);
        } else if (HBaseManagerConstants.SPLIT_3.equals(tableDescDto.getSplitWay())) {
            final SplitGoEnum splitGoEnum = SplitGoEnum.getSplitGoEnum(tableDescDto.getSplitGo());
            if (splitGoEnum == null) {
                return error("?????????Key???????????????????????????");
            }
            if (tableDescDto.getNumRegions() < 1) {
                return error("?????????????????????0");
            }

            createTableRes = multiHBaseAdminService.createTable(clusterCode, tableDesc, splitGoEnum, tableDescDto.getNumRegions(), false);
        }

        if (!createTableRes) {
            return error("???????????????HBase???[" + fullTableName + "]???????????????");
        }
        SysUserHbaseTable sysUserHbaseTable = new SysUserHbaseTable();
        sysUserHbaseTable.setClusterAlias(clusterCode);
        sysUserHbaseTable.setTableId(tableId);
        sysUserHbaseTable.setTableName(fullTableName);
        sysUserHbaseTable.setNamespaceName(HMHBaseConstant.getNamespaceName(fullTableName));
        sysUserHbaseTable.setUserId(ShiroUtils.getUserId());
        userHbaseTableService.insertSysUserHbaseTable(sysUserHbaseTable);

        if (!"admin".equals(ShiroUtils.getLoginName())) {
            SysUserHbaseTable sysAdminHbaseTable = new SysUserHbaseTable();
            sysAdminHbaseTable.setClusterAlias(clusterCode);
            sysAdminHbaseTable.setTableId(tableId);
            sysAdminHbaseTable.setTableName(fullTableName);
            sysAdminHbaseTable.setNamespaceName(HMHBaseConstant.getNamespaceName(fullTableName));
            sysAdminHbaseTable.setUserId(userService.selectUserByLoginName("admin").getUserId());
            userHbaseTableService.insertSysUserHbaseTable(sysAdminHbaseTable);
        }

        return success("HBase???[" + fullTableName + "]???????????????");
    }

    /**
     * ??????HBase
     */
    @GetMapping("/edit/{tableId}")
    public String edit(@PathVariable("tableId") String tableId, ModelMap mmap) {
        final String tableName = StrEnDeUtils.decrypt(tableId);

        String clusterCode = clusterCodeOfCurrentSession();
        HTableDesc tableDesc = multiHBaseAdminService.getHTableDesc(clusterCode, tableName);
        TableDescDto tableDescDto = new TableDescDto().convertFor(tableDesc);
        mmap.put("tableDescDto", tableDescDto);
        mmap.put("tags", selectHBaseTagsByTable(tableDescDto));
        return prefix + "/edit";
    }


    /**
     * ????????????HBase
     */
    @RequiresPermissions("hbase:table:edit")
    @Log(title = "HBase???????????????", businessType = BusinessType.UPDATE)
    @PostMapping("/edit")
    @ResponseBody
    public AjaxResult editSave(TableDescDto tableDescDto) {
        String clusterCode = clusterCodeOfCurrentSession();
        String tableName = StrEnDeUtils.decrypt(tableDescDto.getTableId());
        tableDescDto.setTableName(tableName);
        tableDescDto.setLastUpdateBy(ShiroUtils.getLoginName());
        tableDescDto.setLastUpdateTimestamp(System.currentTimeMillis());
        String currentDisableFlag = tableDescDto.getDisableFlag();
        if (HBASE_TABLE_ENABLE_FLAG.equals(currentDisableFlag) && multiHBaseAdminService.isTableDisabled(clusterCode, tableName)) {
            multiHBaseAdminService.enableTable(clusterCode, tableName);
        }
        if (HBASE_TABLE_DISABLE_FLAG.equals(currentDisableFlag) && !multiHBaseAdminService.isTableDisabled(clusterCode, tableName)) {
            multiHBaseAdminService.disableTable(clusterCode, tableName);
        }
        HTableDesc tableDesc = tableDescDto.convertTo();
        multiHBaseAdminService.modifyTableProps(clusterCode, tableDesc);
        return success();
    }

    /**
     * ????????????HBase???????????????
     */
    @RequiresPermissions("hbase:table:edit")
    @Log(title = "HBase???????????????", businessType = BusinessType.UPDATE)
    @PostMapping("/changeDisableStatus")
    @ResponseBody
    public AjaxResult changeDisableStatus(QueryHBaseTableForm form) {
        String clusterCode = clusterCodeOfCurrentSession();
        boolean changeTableDisabledStatusRes = false;
        String tableName = parseTableNameFromTableId(form.getTableId());

        if (multiHBaseAdminService.isTableDisabled(clusterCode, tableName)) {
            changeTableDisabledStatusRes = multiHBaseAdminService.enableTable(clusterCode, tableName);
        }
        if (!multiHBaseAdminService.isTableDisabled(clusterCode, tableName)) {
            changeTableDisabledStatusRes = multiHBaseAdminService.disableTable(clusterCode, tableName);
        }
        if (!changeTableDisabledStatusRes) {
            return error("???????????????????????????????????????");
        }
        return success();
    }


    /**
     * ??????HBase???
     */
    @RequiresPermissions("hbase:table:remove")
    @Log(title = "HBase?????????", businessType = BusinessType.DELETE)
    @PostMapping("/remove")
    @ResponseBody
    public AjaxResult remove(String tableId) {
        final String clusterCode = clusterCodeOfCurrentSession();
        final String tableName = StrEnDeUtils.decrypt(tableId);

        if (!multiHBaseAdminService.isTableDisabled(clusterCode, tableName)) {
            return error("????????????????????????????????????");
        }

        boolean deleteTableDisabledStatusRes = multiHBaseAdminService.deleteTable(clusterCode, tableName);
        if (!deleteTableDisabledStatusRes) {
            return error("???????????????????????????????????????");
        }
        SysUserHbaseTable sysUserHbaseTable = new SysUserHbaseTable();
        sysUserHbaseTable.setClusterAlias(clusterCode);
        sysUserHbaseTable.setTableId(tableId);

        userHbaseTableService.deleteSysUserHbaseTableByTableId(sysUserHbaseTable);

        return success();
    }


    /**
     * ??????HBase????????????
     */
    @RequiresPermissions("hbase:table:clear")
    @Log(title = "HBase???????????????", businessType = BusinessType.DELETE)
    @PostMapping("/truncatePreserveTable")
    @ResponseBody
    public AjaxResult truncatePreserveTable(String tableId) {
        final String tableName = parseTableNameFromTableId(tableId);
        final String clusterCode = clusterCodeOfCurrentSession();

        if (!multiHBaseAdminService.isTableDisabled(clusterCode, tableName)) {
            return error("??????????????????????????????????????????");
        }

        multiHBaseAdminService.truncatePreserve(clusterCode, tableName);
        return success("HBase???[" + tableName + "]???????????????????????????");
    }


    private List<NamespaceDescDto> getAllNamespaces() {
        return multiHBaseAdminService.listAllNamespaceDesc(clusterCodeOfCurrentSession())
                .stream().map(namespaceDesc -> new NamespaceDescDto().convertFor(namespaceDesc))
                .collect(Collectors.toList());
    }


    /**
     * ???????????????
     *
     * @param tableDescDto ??????????????????
     * @return ?????????
     */
    private List<SysHbaseTag> selectHBaseTagsByTable(TableDescDto tableDescDto) {
        List<SysHbaseTag> hbaseTags = getSysHbaseTagByLongIds(tableDescDto.getTagIds());
        List<SysHbaseTag> tags = sysHbaseTagService.selectAllSysHbaseTagList();
        for (SysHbaseTag tag : tags) {
            for (SysHbaseTag hbaseTag : hbaseTags) {
                if (tag.getTagId().longValue() == hbaseTag.getTagId().longValue()) {
                    tag.setFlag(true);
                    break;
                }
            }
        }
        return tags;
    }

    private List<CxSelect> getTableSelectData(String tableName) {
        List<String> families = multiHBaseAdminService.getColumnFamilyDesc(clusterCodeOfCurrentSession(), tableName)
                .stream().map(ColumnFamilyDesc::getFamilyName).collect(Collectors.toList());
        List<CxSelect> cxTableInfoList = new ArrayList<>();
        CxSelect cxSelectTable = new CxSelect();
        cxSelectTable.setN(tableName);
        cxSelectTable.setV(tableName);
        List<CxSelect> tempFamilyList = new ArrayList<>();
        for (String family : families) {
            CxSelect cxSelectFamily = new CxSelect();
            cxSelectFamily.setN(family);
            cxSelectFamily.setV(family);
            tempFamilyList.add(cxSelectFamily);
        }
        cxSelectTable.setS(tempFamilyList);
        cxTableInfoList.add(cxSelectTable);
        return cxTableInfoList;
    }

}
