package io.toxa108.blitzar.storage.database.manager.user;

public class BzUser implements User {
    private final String login;
    private final String encryptPassword;

    public BzUser(final String login, final String encryptPassword) {
        this.login = login;
        this.encryptPassword = encryptPassword;
    }

    @Override
    public String login() {
        return login;
    }

    @Override
    public String password() {
        return encryptPassword;
    }
}
