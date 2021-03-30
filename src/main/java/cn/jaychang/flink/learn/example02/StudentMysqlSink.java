package cn.jaychang.flink.learn.example02;

import cn.jaychang.flink.learn.common.model.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class StudentMysqlSink extends RichSinkFunction<Student> {

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private Connection connection;
    private PreparedStatement ps;

    public StudentMysqlSink(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        ps = connection.prepareStatement("INSERT INTO t_student(`id`,`name`,`age`) VALUES (?,?,?)");
    }

    @Override
    public void close() throws Exception {
        ps.close();
        connection.close();
    }

    @Override
    public void invoke(Student student, Context context) throws Exception {
        ps.setLong(1,student.getId());
        ps.setString(2,student.getName());
        ps.setInt(3,student.getAge());
        int executeUpdate = ps.executeUpdate();
    }
}
