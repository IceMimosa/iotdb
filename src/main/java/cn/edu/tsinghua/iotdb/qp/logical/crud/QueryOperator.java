package cn.edu.tsinghua.iotdb.qp.logical.crud;

import cn.edu.tsinghua.iotdb.qp.logical.Operator;

/**
 * this class extends {@code RootOperator} and process getIndex statement
 * 
 */
public class QueryOperator extends SFWOperator {

    public QueryOperator(int tokenIntType) {
        super(tokenIntType);
        operatorType = Operator.OperatorType.QUERY;
    }

    private long unit;
    private long origin;
    private FilterOperator intervals;
    private boolean isGroupBy = false;

    public void setGroupBy(boolean isGroupBy) {
        this.isGroupBy = isGroupBy;
    }

    public boolean isGroupBy() {
        return isGroupBy;
    }

    public long getUnit() {
        return unit;
    }

    public void setUnit(long unit) {
        this.unit = unit;
    }

    public void setOrigin(long origin) {
        this.origin = origin;
    }

    public long getOrigin() {
        return origin;
    }

    public void setIntervals(FilterOperator intervals) {
        this.intervals = intervals;
    }

    public FilterOperator getIntervals() {
        return intervals;
    }

}
