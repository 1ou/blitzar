package io.toxa108.blitzar.storage.database.manager.user;

public interface UserManager {
    /**
     * Create user
     * @param login login
     * @param password password
     * @return user
     */
    User createUser(String login, String password);

    /**
     * Clear
     */
    void clear();
}
