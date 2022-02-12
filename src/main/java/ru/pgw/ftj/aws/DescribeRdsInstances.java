package ru.pgw.ftj.aws;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClient;
import com.amazonaws.services.rds.model.AmazonRDSException;
import com.amazonaws.services.rds.model.DBInstance;
import com.amazonaws.services.rds.model.DescribeDBInstancesResult;
import java.util.List;

public class DescribeRdsInstances {

    public static void main(String[] args) {
        AmazonRDS rdsClient = AmazonRDSClient.builder()
            .withRegion(Regions.US_EAST_1)
            .build();

        describeInstances(rdsClient);
    }

    public static void describeInstances(AmazonRDS rdsClient) {

        try {

            DescribeDBInstancesResult response = rdsClient.describeDBInstances();

            List<DBInstance> instanceList = response.getDBInstances();

            for (DBInstance instance : instanceList) {
                System.out.println("Instance Identifier is: " + instance.getDBInstanceIdentifier());
                System.out.println("The Engine is " + instance.getEngine());
                System.out.println("Connection endpoint is" + instance.getEndpoint().getAddress());
            }

        } catch (AmazonRDSException e) {
            System.out.println(e.getLocalizedMessage());
            System.exit(1);
        }
    }

}
