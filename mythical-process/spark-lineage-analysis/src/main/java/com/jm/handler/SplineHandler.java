package com.jm.handler;

import com.jm.constants.SplineConstants;
import com.jm.spline.*;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/6/2 10:10
 */
public class SplineHandler {

    public static String findAttrValue(Map<String, String> map) {
        Set<Map.Entry<String, String>> entries = map.entrySet();
        String value = "";
        for (Map.Entry<String, String> entry : entries) {
            value = entry.getValue();
        }
        return value;
    }

    /**
     * 在express中递归找到最后的attr
     *
     * @param id
     * @param functionsMap
     * @return
     */
    public static List<String> findAttr(String id, Map<String, Functions> functionsMap, List<String> ids, Map<String, Attributes> attributesMap) {
        List<String> attrs = new ArrayList<>();
        List<Map<String, String>> childRefs = functionsMap.get(id).getChildRefs();
        if (CollectionUtils.isNotEmpty(childRefs)) {
            childRefs.stream().forEach(e -> {
                String value = findAttrValue(e);
                if (value.startsWith(SplineConstants.ATTR_START)) {
                    if (ids.contains(value)) {
                        attrs.add(value);
                    } else {
                        Attributes attributes = attributesMap.get(value);
                        /**
                         * todo 只支持字段一对一
                         * "childRefs":[
                         *                     {
                         *                         "id":"expr-28"
                         *                     }
                         *                 ]
                         * 只支持 childRefs中只有一个childRef的情况
                         *
                         */
                        value = findAttrValue(attributes.getChildRefs().get(0));
                        attrs.addAll(findAttr(value, functionsMap, ids, attributesMap));
                    }
                } else if (value.startsWith(SplineConstants.EXPR_START)) {
                    attrs.addAll(findAttr(value, functionsMap, ids, attributesMap));
                }
            });
        }
        return attrs;
    }

    public SplineInfo transform(ExecutionPlan executionPlan) {
        SplineInfo splineInfo = new SplineInfo();

        List<Reads> reads = executionPlan.getOperations().getReads();
        Write write = executionPlan.getOperations().getWrite();
        if (write == null && CollectionUtils.isEmpty(reads)) {
            throw new RuntimeException("血缘json格式出现异常，找不到write、reads");
        }
        List<SplineNodeInfo> readNodeInfos = reads.stream().map(e -> {
            String sourcePath = "";
            if (e.getExtra().getSourceType() == null) {
                String input = e.getInputSources().get(0);
                if (input.contains(SplineConstants.POINT_PARQUET)) {
                    sourcePath = input.substring(0, input.lastIndexOf("/"));
                }
            } else {
                sourcePath = e.getInputSources().get(0);
            }
            return new SplineNodeInfo(sourcePath, e.getExtra().getSourceType() == null ? SplineConstants.DELTA : e.getExtra().getSourceType(), e.getOutput(), e.getParams());
        }).collect(Collectors.toList());

        splineInfo.setWriteNodeInfo(new SplineNodeInfo(write.getOutputSource(), write.getExtra().getDestinationType(), null, write.getParams()));
        splineInfo.setReadNodeInfo(readNodeInfos);

        return splineInfo;
    }

    /**
     * 获取字段血缘信息
     *
     * @param executionPlan
     * @return
     */
    public List<SplineEdgeInfo> transformSplineEdgeInfo(ExecutionPlan executionPlan) {
        List<SplineEdgeInfo> splineEdgeInfos = new ArrayList<>();

        Map<String, Attributes> attributesMap = executionPlan.getAttributes().stream()
                .collect(Collectors.toMap(Attributes::getId, Function.identity()));

        List<Functions> expressions = new ArrayList<>();
        List<Functions> functions = executionPlan.getExpressions() != null ? executionPlan.getExpressions().getFunctions() : new ArrayList<>();
        List<Functions> constants = executionPlan.getExpressions() != null ? executionPlan.getExpressions().getConstants() : new ArrayList<>();
        expressions.addAll(functions);
        expressions.addAll(constants);

        List<Reads> reads = executionPlan.getOperations().getReads();

        List<String> readIds = new ArrayList<>();
        reads.stream().forEach(e -> readIds.addAll(e.getOutput()));

        Map<String, Functions> functionsMap = expressions.stream().collect(Collectors.toMap(Functions::getId, Function.identity()));
        Map<String, Other> otherMap = executionPlan.getOperations().getOther().stream().collect(Collectors.toMap(Other::getId, Function.identity()));


        Write write = executionPlan.getOperations().getWrite();
        Other other = otherMap.get(write.getChildIds().get(0));

        other.getOutput().stream().forEach(x -> {
            Attributes attribute = attributesMap.get(x);

            if (CollectionUtils.isEmpty(functions) && CollectionUtils.isEmpty(attribute.getChildRefs())) {
                List<String> attrList = new ArrayList<>();
                attrList.add(x);
                SplineEdgeInfo splineEdgeInfo = new SplineEdgeInfo(attrList, x);
                splineEdgeInfos.add(splineEdgeInfo);
            } else {
                List<Map<String, String>> childRefs = attribute.getChildRefs();
                List<String> attrList = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(childRefs)) {
                    childRefs.stream().forEach(a -> {
                        String value = findAttrValue(a);
                        if (value.startsWith(SplineConstants.ATTR_START)) {
                            attrList.add(value);
                        } else if (value.startsWith(SplineConstants.EXPR_START)) {
                            attrList.addAll(findAttr(value, functionsMap, readIds, attributesMap));
                        }
                    });
                } else {
                    attrList.add(x);
                }
                SplineEdgeInfo splineEdgeInfo = new SplineEdgeInfo(attrList, x);
                splineEdgeInfos.add(splineEdgeInfo);
            }
        });
        return splineEdgeInfos;
    }
}
