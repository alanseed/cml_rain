#ifndef MONGO_CLIENT_MANAGER_H
#define MONGO_CLIENT_MANAGER_H

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>

class MongoClientManager {
public:
    // Deleted copy constructor and assignment operator to enforce Singleton
    MongoClientManager(const MongoClientManager&) = delete;
    MongoClientManager& operator=(const MongoClientManager&) = delete;

    // Static method to access the single MongoDB client
    static mongocxx::client& get_client() {
        static MongoClientManager instance; // Ensures the client is initialized once
        return instance.client;
    }

private:
    // Private constructor to prevent multiple instances
    MongoClientManager() 
        : mongo_instance{},  // Initializes the MongoDB instance
          uri("mongodb://localhost:27017"), // Sets the URI
          client(uri) {}  // Initializes the MongoDB client with the URI

    // MongoDB objects for instance and client
    mongocxx::instance mongo_instance;
    mongocxx::uri uri;
    mongocxx::client client;
};

#endif // MONGO_CLIENT_MANAGER_H
