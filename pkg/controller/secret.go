package controller

import (
	"fmt"

	"github.com/appscode/go/crypto/rand"
	api "github.com/kubedb/apimachinery/apis/kubedb/v1alpha1"
	"github.com/kubedb/apimachinery/client/clientset/versioned/typed/kubedb/v1alpha1/util"
	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	core_util "kmodules.xyz/client-go/core/v1"
)

const (
	mariadbUser = "root"

	KeyMariaDBUser     = "username"
	KeyMariaDBPassword = "password"
)

func (c *Controller) ensureDatabaseSecret(mariadb *api.MariaDB) error {
	if mariadb.Spec.DatabaseSecret == nil {
		secretVolumeSource, err := c.createDatabaseSecret(mariadb)
		if err != nil {
			return err
		}

		ms, _, err := util.PatchMariaDB(c.ExtClient.KubedbV1alpha1(), mariadb, func(in *api.MariaDB) *api.MariaDB {
			in.Spec.DatabaseSecret = secretVolumeSource
			return in
		})
		if err != nil {
			return err
		}
		mariadb.Spec.DatabaseSecret = ms.Spec.DatabaseSecret
		return nil
	}
	return c.upgradeDatabaseSecret(mariadb)
}

func (c *Controller) createDatabaseSecret(mariadb *api.MariaDB) (*core.SecretVolumeSource, error) {
	authSecretName := mariadb.Name + "-auth"

	sc, err := c.checkSecret(authSecretName, mariadb)
	if err != nil {
		return nil, err
	}
	if sc == nil {
		randPassword := ""

		// if the password starts with "-", it will cause error in bash scripts (in mariadb-tools)
		for randPassword = rand.GeneratePassword(); randPassword[0] == '-'; {
		}

		secret := &core.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:   authSecretName,
				Labels: mariadb.OffshootSelectors(),
			},
			Type: core.SecretTypeOpaque,
			StringData: map[string]string{
				KeyMariaDBUser:     mariadbUser,
				KeyMariaDBPassword: randPassword,
			},
		}
		if _, err := c.Client.CoreV1().Secrets(mariadb.Namespace).Create(secret); err != nil {
			return nil, err
		}
	}
	return &core.SecretVolumeSource{
		SecretName: authSecretName,
	}, nil
}

// This is done to fix 0.8.0 -> 0.9.0 upgrade due to
// https://github.com/kubedb/mariadb/pull/115/files#diff-10ddaf307bbebafda149db10a28b9c24R17 commit
func (c *Controller) upgradeDatabaseSecret(mariadb *api.MariaDB) error {
	meta := metav1.ObjectMeta{
		Name:      mariadb.Spec.DatabaseSecret.SecretName,
		Namespace: mariadb.Namespace,
	}

	_, _, err := core_util.CreateOrPatchSecret(c.Client, meta, func(in *core.Secret) *core.Secret {
		if _, ok := in.Data[KeyMariaDBUser]; !ok {
			if val, ok2 := in.Data["user"]; ok2 {
				in.StringData = map[string]string{KeyMariaDBUser: string(val)}
			}
		}
		return in
	})
	return err
}

func (c *Controller) checkSecret(secretName string, mariadb *api.MariaDB) (*core.Secret, error) {
	secret, err := c.Client.CoreV1().Secrets(mariadb.Namespace).Get(secretName, metav1.GetOptions{})
	if err != nil {
		if kerr.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	if secret.Labels[api.LabelDatabaseKind] != api.ResourceKindMariaDB ||
		secret.Labels[api.LabelDatabaseName] != mariadb.Name {
		return nil, fmt.Errorf(`intended secret "%v/%v" already exists`, mariadb.Namespace, secretName)
	}
	return secret, nil
}
