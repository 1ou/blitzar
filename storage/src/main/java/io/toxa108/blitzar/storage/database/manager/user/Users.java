package io.toxa108.blitzar.storage.database.manager.user;

public interface Users {
    /**
     * Create user
     * @param login login
     * @param password password
     * @return user
     */
    User create(String login, String password);

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
