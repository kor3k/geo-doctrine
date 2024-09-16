<?php declare(strict_types=1);

namespace Brick\Geo\Doctrine;

use Brick\Geo\Engine\DatabaseEngine;
use Brick\Geo\Engine\GeometryParameter;
use Brick\Geo\Exception\GeometryEngineException;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\Exception as DBALException;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Platforms\MySQLPlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Statement;
use Doctrine\ORM\EntityManagerInterface;

class DoctrineEngine extends DatabaseEngine
{
    /** @var Statement[] */
    private array $statements = [];

    public function __construct(
        private readonly EntityManagerInterface|Connection $conn,
        bool $useProxy = true,
    ) {
        parent::__construct($useProxy);
    }

    protected function getDBAL(): Connection
    {
        if ($this->conn instanceof Connection) {
            return $this->conn;
        }

        return $this->conn->getConnection();
    }

    protected function executeQuery(string $query, array $parameters): array
    {
        try {
            if (!isset($this->statements[$query])) {
                $this->statements[$query] = $this->getDBAL()->prepare($query);
            }

            $statement = $this->statements[$query];

            $index = 1;

            foreach ($parameters as $parameter) {
                if ($parameter instanceof GeometryParameter) {
                    $statement->bindValue($index++, $parameter->data, $parameter->isBinary ? ParameterType::LARGE_OBJECT : ParameterType::STRING);
                    $statement->bindValue($index++, $parameter->srid, ParameterType::INTEGER);
                } else {
                    if (is_int($parameter)) {
                        $type = ParameterType::INTEGER;
                    } else {
                        $type = ParameterType::STRING;
                    }

                    $statement->bindValue($index++, $parameter, $type);
                }
            }

            $result = $statement->getWrappedStatement()->execute();
            $result = $result->fetchNumeric();
        } catch (DBALException $e) {
            $errorClass = substr((string) $e->getSQLState(), 0, 2);

            // 42XXX = syntax error or access rule violation; reported on undefined function.
            // 22XXX = data exception; reported by MySQL 5.7 on unsupported geometry.
            if ('42' === $errorClass || '22' === $errorClass) {
                throw GeometryEngineException::operationNotSupportedByEngine($e);
            }

            throw $e;
        } catch (\Throwable $e) {
            $errorClass = substr((string) $e->getCode(), 0, 2);

            // 42XXX = syntax error or access rule violation; reported on undefined function.
            // 22XXX = data exception; reported by MySQL 5.7 on unsupported geometry.
            if ('42' === $errorClass || '22' === $errorClass) {
                throw GeometryEngineException::operationNotSupportedByEngine($e);
            }

            throw $e;
        }

        assert(false !== $result);

        return $result;
    }

    protected function getGeomFromWKBSyntax(): string
    {
        if ($this->isMySQL()) {
            return 'ST_GeomFromWKB(BINARY ?, ?)';
        }

        return parent::getGeomFromWKBSyntax();
    }

    protected function getParameterPlaceholder(string|float|int $parameter): string
    {
        if ($this->isPostgreSQL()) {
            if (is_int($parameter)) {
                // https://stackoverflow.com/q/66625661/759866
                // https://externals.io/message/113521
                return 'CAST (? AS INTEGER)';
            }
        }

        return parent::getParameterPlaceholder($parameter);
    }

    protected function isMySQL(): bool
    {
        return $this->getDBAL()->getDatabasePlatform() instanceof MySQLPlatform;
    }

    protected function isPostgreSQL(): bool
    {
        return $this->getDBAL()->getDatabasePlatform() instanceof PostgreSQLPlatform;
    }
}
