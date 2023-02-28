package org.apache.flink;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.databend.DatabendDynamicTableFactory;
import org.apache.flink.connector.databend.DatabendDynamicTableSink;
import org.apache.flink.connector.databend.catalog.DatabendCatalog;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;


public class TestDatabendDynamicTableFactory {

    @Mock
    DynamicTableFactory.Context mockContext;

    @Mock
    ResolvedCatalogTable mockResolvedCatalogTable;

    @Mock
    Schema mockSchema;

    @Mock
    UniqueConstraint mockUniqueConstraint;

    @Mock
    TableSchema mockTableSchema;

    @Mock
    DataType mockDataType;

    @Mock
    DynamicTableSink.Context mockTableSinkContext;

    @Mock
    DynamicTableSink mockDynamicTableSink;

    @Test
    public void testCreateDynamicTableSink() {
        MockitoAnnotations.initMocks(this);

        // set up mock objects
        Map<String, String> options = new HashMap<>();
        options.put("key1", "value1");
        options.put("key2", "value2");
        when(mockResolvedCatalogTable.getOptions()).thenReturn(options);
//        when(mockResolvedCatalogTable.getSchema()).thenReturn(mockSchema);
//        when(mockSchema.toTableSchema()).thenReturn(mockTableSchema);
        when(mockTableSchema.getFieldNames()).thenReturn(new String[]{"field1", "field2"});
        when(mockContext.getCatalogTable()).thenReturn(mockResolvedCatalogTable);
//        when(mockTableSinkContext.getPhysicalRowDataType()).thenReturn(mockDataType);

        Configuration configuration = new Configuration();
        configuration.setString("dml-options", "batch_size=100");
        configuration.setString("url", "databend://localhost:8000");
        configuration.setString("table-name", "test");
        ReadableConfig readableConfig = configuration;
        DatabendDynamicTableFactory databendDynamicTableFactory = new DatabendDynamicTableFactory();
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(databendDynamicTableFactory, mockContext);
        when(helper.getOptions()).thenReturn(readableConfig);

//        when(mockSchema.getPrimaryKey()).thenReturn(Optional.of(mockUniqueConstraint));
//        when(mockUniqueConstraint.getColumns()).thenReturn(Arrays.asList("field1"));


        DynamicTableSink tableSink = databendDynamicTableFactory.createDynamicTableSink(mockContext);
        assertNotNull(tableSink);
        assertTrue(tableSink instanceof DatabendDynamicTableSink);

        DatabendCatalog databendCatalog = new DatabendCatalog("databend", options);
        ReadableConfig config = helper.getOptions();
    }
}
