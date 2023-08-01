package com.jm.demo.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/8/1 10:39
 */
public class ParquetHelper {

    public static void main(String[] args) {
        String parquetFilePath = "hdfs://192.168.110.42:8020/user/hive/warehouse/dwd.db/company_info/_delta_log/00000000000000001710.checkpoint.parquet";

        read(parquetFilePath);
    }

    public static void read(String parquetFilePath) {
        ParquetFileReader fileReader = null;
        try {
            fileReader = ParquetFileReader.open(new Configuration(), new Path(parquetFilePath));
            FileMetaData fileMetaData = fileReader.getFooter().getFileMetaData();
            MessageType schema = fileMetaData.getSchema();

//            ParquetMetadata footer = fileReader.getFooter();
//            List<BlockMetaData> blocks = footer.getBlocks();
//            blocks.stream().forEach(e->{
//                System.out.println("rowcount = " +e.getRowCount());
//                System.out.println("path = "+ e.getPath());
//                System.out.println(e.getTotalByteSize());
//            });
//            // 获取元数据信息
//            System.out.println("总记录数: " + fileMetaData.getKeyValueMetaData());
//            System.out.println("Schema: " + fileMetaData.getSchema());
//            // 还可以访问其他元数据属性，如列信息、压缩方式等

            List<Group> groups = new ArrayList<>();
            PageReadStore pageReadStore = null;
            while ((pageReadStore = fileReader.readNextFilteredRowGroup()) != null) {
                long rowCount = pageReadStore.getRowCount();
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                RecordReader<Group> recordReader = columnIO.getRecordReader(pageReadStore, new GroupRecordConverter(schema));
                for (int i = 0; i < rowCount; i++) {
                    Group g = recordReader.read();

                    System.out.println(" group = " + g);
                    groups.add(g);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        } finally {
            if (fileReader != null) {
                try {
                    fileReader.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static void write(String parquetFilePath) {
        try {
            String schemaString = "message TestData {\n" +
                    "  required int32 id;\n" +
                    "  required binary name;\n" +
                    "}";
            MessageType schema = MessageTypeParser.parseMessageType(schemaString);
            // 创建GroupFactory和示例数据
            GroupFactory groupFactory = new SimpleGroupFactory(schema);
            Group group = groupFactory.newGroup();
            group.append("id", 1);
            group.append("name", "John Doe");

            Configuration configuration = new Configuration();
            GroupWriteSupport.setSchema(schema, configuration);

            try (ParquetWriter<Group> writer = new ParquetWriter<>(new Path(parquetFilePath), new GroupWriteSupport(), ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME, ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, true, false, ParquetWriter.DEFAULT_WRITER_VERSION, configuration)) {
                writer.write(group);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
