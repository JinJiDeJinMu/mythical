package com.jm.spline;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/3/13 10:35
 */
@Data
public class ProjectInfo {

    private String id;
    private String _attrId;
    private String _exprId;

    public String getValue() {
        if (StringUtils.isNotBlank(get_attrId())) {
            return get_attrId();
        } else if (StringUtils.isNotBlank(get_exprId())) {
            return get_exprId();
        }
        return getId();
    }
}
