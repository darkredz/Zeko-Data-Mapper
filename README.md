# Zeko-Data-Mapper
A lightweight data mapper library in Kotlin that helps to map the result of queries from your normalized dataset(from RDBMS or any source) back into relational mapped Hash maps

To use, add these to your maven pom.xml


    <repositories>
      <repository>
        <id>Zeko-Data-Mapper-mvn-repo</id>
        <url>https://raw.github.com/darkredz/Zeko-Data-Mapper/mvn-repo/</url>
        <snapshots>
          <enabled>true</enabled>
          <updatePolicy>always</updatePolicy>
        </snapshots>
      </repository>
    </repositories>
    
    <dependency>
      <groupId>com.zeko.model</groupId>
      <artifactId>data-mapper</artifactId>
      <version>1.0</version>
    </dependency>
    
