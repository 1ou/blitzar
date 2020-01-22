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
     * Authorize user
     * @param login login
     * @param password password
     * @return user
     * @throws AccessDeniedException user can't be authorized
     */
    User authorize(String login, String password) throws AccessDeniedException;

    /**
     * Clear
     */
    void clear();
}
